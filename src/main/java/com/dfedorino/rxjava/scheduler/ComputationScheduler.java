package com.dfedorino.rxjava.scheduler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Шедулер для CPU-операций (FixedThreadPool)
 */
public class ComputationScheduler implements Scheduler {
    private final ExecutorService executorService;
    private volatile boolean isShutdown;

    public ComputationScheduler() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public ComputationScheduler(int threadCount) {
        this(Executors.newFixedThreadPool(threadCount, new ComputationThreadFactory()));
    }

    ComputationScheduler(ExecutorService executorService) {
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

    private static class ComputationThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("rxjava-computation-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
