package org.java_websocket.util;

public class ExceptionUtil {

    private ExceptionUtil() {}

    public static Exception generateForStack(Thread t) {
        Exception e = new Exception("Stack Trace");
        e.setStackTrace(t.getStackTrace());
        return e;
    }



}