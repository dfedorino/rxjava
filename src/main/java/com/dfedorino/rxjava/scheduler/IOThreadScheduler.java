package com.dfedorino.rxjava.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Шедулер для I/O-операций на базе CachedThreadPool.
 * Создаёт новые потоки по мере необходимости и переиспользует бездействующие.
 * Подходит для блокирующих операций (сетевые запросы, файловый I/O, запросы к БД).
 */
public class IOThreadScheduler implements Scheduler {
    
    private final ExecutorService executorService;
    private volatile boolean isShutdown;
    
    public IOThreadScheduler() {
        this(Executors.newCachedThreadPool(new IoThreadFactory()));
    }
    
    IOThreadScheduler(ExecutorService executorService) {
        this.executorService = executorService;
        this.isShutdown = false;
    }
    
    @Override
    public void execute(Runnable task) {
        if (isShutdown) {
            throw new RejectedExecutionException("IOThreadScheduler is shut down, cannot accept new tasks");
        }
        try {
            executorService.execute(task);
        } catch (RejectedExecutionException e) {
            throw new RejectedExecutionException("IOThreadScheduler rejected task (no threads available)", e);
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
    
    private static class IoThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(0);
        
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("rxjava-io-" + counter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        }
    }
}
