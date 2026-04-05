package com.dfedorino.rxjava.operators.threading;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.scheduler.Scheduler;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class ObserveOnObservable<T> extends Observable<T> {
    private final Observable<T> source;
    private final Scheduler scheduler;

    public ObserveOnObservable(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(Observer<T> observer) {
        ObserveOnObserver<T> parent = new ObserveOnObserver<>(observer, scheduler);
        observer.onSubscribe(parent);
        source.subscribe(parent);
    }

    /**
     * Планирует события в Scheduler и доставляет через drain-цикл
     */
    static final class ObserveOnObserver<T> implements Observer<T>, Disposable {
        private final Observer<T> downstream;
        private final Scheduler scheduler;
        private final AtomicReference<Disposable> upstream = new AtomicReference<>();
        private final AtomicBoolean disposed = new AtomicBoolean();
        private final Queue<T> queue = new ConcurrentLinkedQueue<>();
        private final AtomicInteger wip = new AtomicInteger();
        private volatile Throwable error;
        private volatile boolean done;

        ObserveOnObserver(Observer<T> downstream, Scheduler scheduler) {
            this.downstream = downstream;
            this.scheduler = scheduler;
        }

        @Override
        public void onSubscribe(Disposable d) {
            Disposable existing = upstream.getAndSet(d);
            if (existing != null) d.dispose();
        }

        @Override
        public void onNext(T item) {
            if (disposed.get() || done) return;
            queue.offer(item);
            schedule();
        }

        @Override
        public void onError(Throwable t) {
            if (disposed.get() || done) return;
            error = t;
            done = true;
            schedule();
        }

        @Override
        public void onComplete() {
            if (disposed.get() || done) return;
            done = true;
            schedule();
        }

        /**
         * Запускает drain-задачу в Scheduler, если другой ещё не выполняется (wip == 0).
         */
        private void schedule() {
            if (wip.getAndIncrement() == 0) {
                try {
                    scheduler.execute(this::drain);
                } catch (Exception e) {
                    if (!done) {
                        error = e;
                        done = true;
                    }
                    if (wip.decrementAndGet() > 0) {
                        scheduler.execute(this::drain);
                    }
                }
            }
        }

        /**
         * Последовательно извлекает элементы из очереди и доставляет их в downstream.
         * Завершается, когда очередь пуста и источник завершён, или при отписке.
         */
        private void drain() {
            int missed = 1;
            for (; ; ) {
                if (checkTerminated()) return;
                T item = queue.poll();
                if (item == null) {
                    if (done) {
                        if (checkTerminated()) return;
                    }
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) return;
                    continue;
                }
                if (!disposed.get()) downstream.onNext(item);
                missed--;
            }
        }

        /**
         * Проверяет терминальное состояние: отписка, ошибка или завершение.
         * Если очередь пуста и done=true — вызывает onComplete на downstream.
         */
        private boolean checkTerminated() {
            if (disposed.get()) {
                queue.clear();
                return true;
            }
            if (done) {
                Throwable e = error;
                if (e != null) {
                    disposed.set(true);
                    downstream.onError(e);
                    return true;
                }
                if (queue.isEmpty()) {
                    disposed.set(true);
                    downstream.onComplete();
                    return true;
                }
            }
            return false;
        }

        @Override
        public void dispose() {
            if (disposed.compareAndSet(false, true)) {
                Disposable d = upstream.get();
                if (d != null) d.dispose();
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed.get();
        }
    }
}
