package org.java_websocket.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import org.java_websocket.handshake.ServerHandshake;
import org.java_websocket.util.ExceptionUtil;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class WebSocketJsonClient extends WebSocketClient {

    private final Logger log = LoggerFactory.getLogger(WebSocketJsonClient.class);

    private final HashMap<String, Function<JSONObject, JSONObject>> eventHandlers = new HashMap<>();
    private final ArrayList<Supplier<Boolean>> connectedHandlers = new ArrayList<>();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final Random r = new Random();
    private boolean connected = false;

    private final Cache<Integer, CompletableFuture<JSONObject>> outCache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .removalListener(e -> {
                if (e.getCause() == RemovalCause.EXPIRED && e.getValue() instanceof CompletableFuture) {
                    ((CompletableFuture<?>)e.getValue())
                            .completeExceptionally(new SocketTimeoutException("No web socket response!"));
                }
            })
            .build();


    public WebSocketJsonClient(String host, int port, String socketId) throws URISyntaxException {
        this(host, port, socketId, (String)null);
    }

    public WebSocketJsonClient(String host, int port, String socketId, HashMap<String, String> httpHeaders) throws URISyntaxException {
        this(host, port, socketId, null, httpHeaders);
    }

    public WebSocketJsonClient(String host, int port, String socketId, String auth) throws URISyntaxException {
        super(new URI(String.format("ws://%s:%d", host, port)));
        addHeader("socket_id", socketId);
        if (auth != null) {
            addHeader("auth", auth);
        }
    }

    public WebSocketJsonClient(String host, int port, String socketId, String auth, HashMap<String, String> httpHeaders) throws URISyntaxException {
        super(new URI(String.format("ws://%s:%d", host, port)), httpHeaders);
        addHeader("socket_id", socketId);
        if (auth != null) {
            addHeader("auth", auth);
        }
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        log.info("Web socket connected");
        connected = true;
        connectedHandlers.removeIf(Supplier::get);
    }

    public void addConnectedHandler(Supplier<Boolean> supplier) {
        connectedHandlers.add(supplier);
    }

    public void removeConnectedHandler(Supplier<Boolean> supplier) {
        connectedHandlers.remove(supplier);
    }

    public void addEventHandler(String event, Function<JSONObject, JSONObject> eventFunction) {
        eventHandlers.put(event, eventFunction);
    }

    public void removeEventHandler(String event) {
        eventHandlers.remove(event);
    }

    public void removeEventHandler(String event, Function<JSONObject, JSONObject> eventFunction) {
        eventHandlers.remove(event, eventFunction);
    }

    @Override
    public void onMessage(String message) {
        if (!message.contains("::"))
            return;

        String event = message.split("::")[0];
        JSONObject contentJson = new JSONObject(message.substring(event.length() + 2));

        int requestId = contentJson.getInt("request_id");
        if (contentJson.getBoolean("is_response")) {
            CompletableFuture<JSONObject> future = outCache.getIfPresent(requestId);
            if (future != null) {
                future.complete(contentJson);
                outCache.invalidate(requestId);
            }
        } else {
            Function<JSONObject, JSONObject> eventFunction = eventHandlers.get(event);
            if (eventFunction != null) {
                AtomicBoolean completed = new AtomicBoolean(false);
                AtomicReference<Thread> t = new AtomicReference<>();

                executorService.submit(() -> {
                    t.set(Thread.currentThread());

                    JSONObject responseJson = eventFunction.apply(contentJson);
                    if (responseJson == null) {
                        responseJson = new JSONObject();
                    }

                    responseJson.put("request_id", requestId);
                    responseJson.put("is_response", true);
                    send(event + "::" + responseJson.toString());
                    completed.set(true);
                });

                scheduledExecutorService.schedule(() -> {
                    if (!completed.get()) {
                        Exception e = ExceptionUtil.generateForStack(t.get());
                        log.error("websocket_" + event + " took too long to process!", e);
                    }
                }, 5, TimeUnit.SECONDS);
            }
        }
    }

    public CompletableFuture<JSONObject> sendSecure(String event, JSONObject content) {
        if (isConnected()) {
            return send(event, content);
        } else {
            CompletableFuture<JSONObject> future = new CompletableFuture<>();
            addConnectedHandler(() -> {
                send(event, content)
                        .exceptionally(e -> {
                            future.completeExceptionally(e);
                            return null;
                        })
                        .thenAccept(future::complete);
                return true;
            });
            return future;
        }
    }

    public synchronized CompletableFuture<JSONObject> send(String event, JSONObject content) {
        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        int id = r.nextInt();

        content.put("request_id", id);
        content.put("is_response", false);
        outCache.put(id, future);
        try {
            send(event + "::" + content.toString());
        } catch (Throwable e) {
            outCache.invalidate(id);
            future.completeExceptionally(e);
            return future;
        }

        return future;
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        if (connected) {
            log.info("Web socket disconnected");
        }
        connected = false;
        scheduledExecutorService.schedule(this::reconnect, 2, TimeUnit.SECONDS);
    }

    @Override
    public void onError(Exception ex) {
        if (!ex.toString().contains("Connection refused")) {
            log.error("Web socket error", ex);
        }
    }

    public boolean isConnected() {
        return connected;
    }

}
