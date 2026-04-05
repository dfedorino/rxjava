package com.dfedorino.rxjava.scheduler;

/**
 * Фабрика синглтонов Scheduler
 */
public final class Schedulers {
    private Schedulers() {}

    private static final class IoHolder {
        static final IOThreadScheduler INSTANCE = new IOThreadScheduler();
    }

    private static final class ComputationHolder {
        static final ComputationScheduler INSTANCE = new ComputationScheduler();
    }

    private static final class SingleHolder {
        static final SingleThreadScheduler INSTANCE = new SingleThreadScheduler();
    }

    public static Scheduler io() {
        return IoHolder.INSTANCE;
    }

    public static Scheduler computation() {
        return ComputationHolder.INSTANCE;
    }

    public static Scheduler single() {
        return SingleHolder.INSTANCE;
    }

}
