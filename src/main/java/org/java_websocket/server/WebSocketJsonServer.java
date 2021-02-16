package org.java_websocket.server;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.util.ExceptionUtil;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

public class WebSocketJsonServer extends WebSocketServer {

    private final Logger log = LoggerFactory.getLogger(WebSocketServer.class);

    private final HashMap<String, BiFunction<String, JSONObject, JSONObject>> eventHandlers = new HashMap<>();
    private final ArrayList<BiFunction<String, ClientHandshake, Boolean>> connectedHandlers = new ArrayList<>();
    private final ArrayList<Function<String, Boolean>> disconnectedHandlers = new ArrayList<>();
    private final BiMap<String, WebSocket> socketBiMap = HashBiMap.create();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final Random r = new Random();
    private final String auth;

    private final Cache<Integer, CompletableFuture<JSONObject>> outCache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .removalListener(e -> {
                if (e.getCause() == RemovalCause.EXPIRED && e.getValue() instanceof CompletableFuture) {
                    ((CompletableFuture<?>)e.getValue())
                            .completeExceptionally(new SocketTimeoutException("No web socket response!"));
                }
            })
            .build();


    public WebSocketJsonServer(InetSocketAddress address) {
        this(address, null);
    }

    public WebSocketJsonServer(InetSocketAddress address, String auth) {
        super(address);
        setReuseAddr(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (InterruptedException e) {
                log.error("Socket disconnect error");
            }
        }, "shutdown_serverstop"));
        this.auth = auth;
    }

    @Override
    public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
        String socketId = clientHandshake.getFieldValue("socket_id");
        String auth = clientHandshake.getFieldValue("auth");
        if (this.auth == null || this.auth.equals(auth)) {
            log.info(socketId + " connected");
            socketBiMap.put(socketId, webSocket);
            connectedHandlers.removeIf(connectedHandlerFunction -> connectedHandlerFunction.apply(socketId, clientHandshake));
        } else {
            log.warn("Unauthorized client: {}", socketId);
            webSocket.close();
        }
    }

    /* returns true if it should be removed immediately after */
    public void addConnectedHandler(BiFunction<String, ClientHandshake, Boolean> function) {
        connectedHandlers.add(function);
    }

    public void removeConnectedHandler(BiFunction<String, ClientHandshake, Boolean> function) {
        connectedHandlers.remove(function);
    }

    public void addDisconnectedHandler(Function<String, Boolean> function) {
        disconnectedHandlers.add(function);
    }

    public void removeDisconnectedHandler(Function<String, Boolean> function) {
        disconnectedHandlers.remove(function);
    }

    public void addEventHandler(String event, BiFunction<String, JSONObject, JSONObject> eventFunction) {
        eventHandlers.put(event, eventFunction);
    }

    public void removeEventHandler(String event) {
        eventHandlers.remove(event);
    }

    public void removeEventHandler(String event, BiFunction<String, JSONObject, JSONObject> eventFunction) {
        eventHandlers.remove(event, eventFunction);
    }

    @Override
    public void onClose(WebSocket webSocket, int i, String s, boolean b) {
        log.info("Web socket disconnected");
        String socketId = socketBiMap.inverse().remove(webSocket);
        disconnectedHandlers.removeIf(disconnectedHandlerFunction -> disconnectedHandlerFunction.apply(socketId));
    }

    public boolean isConnected(String socketId) {
        return socketBiMap.containsKey(socketId);
    }

    @Override
    public void onMessage(WebSocket webSocket, String message) {
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
            BiFunction<String, JSONObject, JSONObject> eventFunction = eventHandlers.get(event);
            if (eventFunction != null) {
                AtomicBoolean completed = new AtomicBoolean(false);
                AtomicReference<Thread> t = new AtomicReference<>();

                executorService.submit(() -> {
                    t.set(Thread.currentThread());

                    JSONObject responseJson = eventFunction.apply(socketBiMap.inverse().get(webSocket), contentJson);
                    if (responseJson == null) {
                        responseJson = new JSONObject();
                    }

                    responseJson.put("request_id", requestId);
                    responseJson.put("is_response", true);
                    webSocket.send(event + "::" + responseJson.toString());
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

    public List<CompletableFuture<JSONObject>> sendBroadcast(String event, JSONObject content, String... excludedSockets) {
        ArrayList<CompletableFuture<JSONObject>> futureList = new ArrayList<>();
        socketBiMap.keySet().stream()
                .filter(socketId -> Arrays.stream(excludedSockets).noneMatch(socketId::equals))
                .map(socketBiMap::get)
                .forEach(webSocket -> futureList.add(send(webSocket, event, content)));
        return futureList;
    }

    public CompletableFuture<JSONObject> sendSecure(String socketId, String event, JSONObject content) {
        WebSocket webSocket = socketBiMap.get(socketId);
        if (webSocket != null) {
            return send(webSocket, event, content);
        } else {
            CompletableFuture<JSONObject> future = new CompletableFuture<>();
            addConnectedHandler((sId, clientHandshake) -> {
                if (sId.equals(socketId)) {
                    send(socketBiMap.get(socketId), event, content)
                            .exceptionally(e -> {
                                future.completeExceptionally(e);
                                return null;
                            })
                            .thenAccept(future::complete);
                    return true;
                }
                return false;
            });
            return future;
        }
    }

    public synchronized CompletableFuture<JSONObject> send(String socketId, String event, JSONObject content) {
        WebSocket webSocket = socketBiMap.get(socketId);
        if (webSocket != null) {
            return send(webSocket, event, content);
        }

        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        future.completeExceptionally(new NoSuchElementException("Invalid socketId"));
        return future;
    }

    public synchronized CompletableFuture<JSONObject> send(WebSocket webSocket, String event, JSONObject content) {
        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        int id = r.nextInt();

        content.put("request_id", id);
        content.put("is_response", false);
        outCache.put(id, future);
        try {
            webSocket.send(event + "::" + content.toString());
        } catch (Throwable e) {
            future.completeExceptionally(e);
            outCache.invalidate(id);
            return future;
        }

        return future;
    }

    public Set<String> getSocketIds() {
        return socketBiMap.keySet();
    }

    public int size() {
        return socketBiMap.size();
    }

    @Override
    public void onError(WebSocket webSocket, Exception ex) {
        log.error("Web socket error", ex);
        scheduledExecutorService.schedule(this::start, 5, TimeUnit.SECONDS);
    }

    @Override
    public void onStart() {
        //Ignore
    }

}
