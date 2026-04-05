package com.dfedorino.rxjava.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Шедулер для CPU-интенсивных вычислений на базе FixedThreadPool.
 * Размер пула равен количеству доступных ядер CPU.
 * Подходит для обработки данных, преобразований, вычислений.
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
        this.isShutdown = false;
    }
    
    @Override
    public void execute(Runnable task) {
        if (isShutdown) {
            throw new RejectedExecutionException("ComputationScheduler is shut down, cannot accept new tasks");
        }
        try {
            executorService.execute(task);
        } catch (RejectedExecutionException e) {
            throw new RejectedExecutionException("ComputationScheduler rejected task (no threads available)", e);
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
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("rxjava-computation-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
