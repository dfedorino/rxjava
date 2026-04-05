package com.dfedorino.rxjava.scheduler;

public interface Scheduler {
    void execute(Runnable task);

    void shutdown();

    boolean isShutdown();
}
