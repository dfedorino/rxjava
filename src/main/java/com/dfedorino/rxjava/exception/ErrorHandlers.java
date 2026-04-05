package com.dfedorino.rxjava.exception;

import java.util.function.Consumer;

public final class ErrorHandlers {
    private static volatile Consumer<Throwable> errorHandler;

    private ErrorHandlers() {}

    public static void setErrorHandler(Consumer<Throwable> handler) {
        errorHandler = handler;
    }

    public static void reset() {
        errorHandler = null;
    }

    public static void onError(Throwable error) {
        Consumer<Throwable> handler = errorHandler;
        if (handler != null) {
            try {
                handler.accept(error);
            } catch (Throwable inner) {
                uncaught(inner);
            }
        } else {
            uncaught(error);
        }
    }

    private static void uncaught(Throwable error) {
        Thread currentThread = Thread.currentThread();
        currentThread.getUncaughtExceptionHandler().uncaughtException(currentThread, error);
    }
}
