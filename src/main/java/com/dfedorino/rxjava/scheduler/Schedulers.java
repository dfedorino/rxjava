package com.dfedorino.rxjava.scheduler;

/**
 * Factory class providing singleton access to commonly used Scheduler instances.
 * Mirrors RxJava's Schedulers class for reuse of thread pools across the application.
 */
public final class Schedulers {

    private Schedulers() {
        throw new UnsupportedOperationException("Utility class");
    }

    private static final class IoHolder {
        static final IOThreadScheduler INSTANCE = new IOThreadScheduler();
    }

    private static final class ComputationHolder {
        static final ComputationScheduler INSTANCE = new ComputationScheduler();
    }

    private static final class SingleHolder {
        static final SingleThreadScheduler INSTANCE = new SingleThreadScheduler();
    }

    /**
     * Returns a singleton I/O scheduler backed by a cached thread pool.
     * Suitable for blocking I/O operations.
     */
    public static Scheduler io() {
        return IoHolder.INSTANCE;
    }

    /**
     * Returns a singleton computation scheduler backed by a fixed thread pool
     * sized to the number of available processors. Suitable for CPU-bound tasks.
     */
    public static Scheduler computation() {
        return ComputationHolder.INSTANCE;
    }

    /**
     * Returns a singleton single-thread scheduler.
     * All tasks execute sequentially on one background thread.
     */
    public static Scheduler single() {
        return SingleHolder.INSTANCE;
    }

    /**
     * Returns a scheduler that executes tasks on the calling thread (FIFO queuing).
     * Currently returns the single scheduler as a close approximation.
     */
    public static Scheduler trampoline() {
        return SingleHolder.INSTANCE;
    }

    /**
     * Returns a scheduler that creates a new thread for each task.
     * Currently returns the I/O scheduler (cached pool spawns threads as needed).
     */
    public static Scheduler newThread() {
        return IoHolder.INSTANCE;
    }

    /**
     * Shuts down all singleton schedulers. Intended for test cleanup or application shutdown.
     */
    public static void shutdownAll() {
        IoHolder.INSTANCE.shutdown();
        ComputationHolder.INSTANCE.shutdown();
        SingleHolder.INSTANCE.shutdown();
    }
}
