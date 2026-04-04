package com.dfedorino.rxjava.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Шедулер с одним выделенным потоком для последовательного выполнения задач.
 * Гарантирует порядок выполнения.
 * Подходит для задач, требующих последовательной обработки.
 */
public class SingleThreadScheduler implements Scheduler {
    
    private final ExecutorService executorService;
    private volatile boolean isShutdown;
    
    public SingleThreadScheduler() {
        this(Executors.newSingleThreadExecutor(new SingleThreadFactory()));
    }
    
    SingleThreadScheduler(ExecutorService executorService) {
        this.executorService = executorService;
        this.isShutdown = false;
    }
    
    @Override
    public void execute(Runnable task) {
        if (isShutdown) {
            throw new RejectedExecutionException("SingleThreadScheduler is shut down, cannot accept new tasks");
        }
        try {
            executorService.execute(task);
        } catch (RejectedExecutionException e) {
            throw new RejectedExecutionException("SingleThreadScheduler rejected task (no threads available)", e);
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
            Thread thread = new Thread(r);
            thread.setName("rxjava-single");
            thread.setDaemon(false);
            return thread;
        }
    }
}
