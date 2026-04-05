package com.dfedorino.rxjava.scheduler;

import java.util.concurrent.*;

/**
 * Шедулер с одним потоком для последовательного выполнения
 */
public class SingleThreadScheduler implements Scheduler {
    private final ExecutorService executorService;
    private volatile boolean isShutdown;

    public SingleThreadScheduler() {
        this(Executors.newSingleThreadExecutor(new SingleThreadFactory()));
    }

    SingleThreadScheduler(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void execute(Runnable task) {
        if (isShutdown) throw new RejectedExecutionException("Scheduler shut down");
        try {
            executorService.execute(task);
        } catch (RejectedExecutionException e) {
            throw new RejectedExecutionException("Task rejected", e);
        }
    }

    @Override
    public void shutdown() {
        if (!isShutdown) {
            isShutdown = true;
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isShutdown() {
        return isShutdown || executorService.isShutdown();
    }

    private static class SingleThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("rxjava-single");
            return t;
        }
    }
}
