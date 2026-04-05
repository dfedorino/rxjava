package com.dfedorino.rxjava.operators.threading;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.scheduler.Scheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Observable wrapper that switches downstream event processing to a specified Scheduler.
 * Each event (onNext, onError, onComplete) is scheduled as a separate task in the Scheduler.
 *
 * @param <T> type of elements in the stream
 */
public class ObserveOnObservable<T> extends Observable<T> {

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
     * Observer wrapper that schedules each event (onNext/onError/onComplete)
     * to be processed in the specified Scheduler.
     *
     * @param <T> type of elements in the stream
     */
    static final class ObserveOnObserver<T> implements Observer<T>, Disposable {

        private final Observer<T> downstream;
        private final Scheduler scheduler;
        private final AtomicReference<Disposable> upstream = new AtomicReference<>();
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        ObserveOnObserver(Observer<T> downstream, Scheduler scheduler) {
            this.downstream = downstream;
            this.scheduler = scheduler;
        }

        @Override
        public void onSubscribe(Disposable d) {
            Disposable existing = upstream.getAndSet(d);
            if (existing != null) {
                d.dispose();
            }
        }

        @Override
        public void onNext(T item) {
            if (disposed.get()) {
                return;
            }
            try {
                scheduler.execute(() -> {
                    if (!disposed.get()) {
                        downstream.onNext(item);
                    }
                });
            } catch (Exception e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (disposed.get()) {
                // TODO: global error handler for undeliverable errors
                return;
            }
            try {
                scheduler.execute(() -> {
                    if (!disposed.get()) {
                        downstream.onError(t);
                    }
                });
            } catch (Exception e) {
                // TODO: global error handler for undeliverable errors
            }
        }

        @Override
        public void onComplete() {
            if (disposed.get()) {
                return;
            }
            try {
                scheduler.execute(() -> {
                    if (!disposed.get()) {
                        disposed.set(true);
                        downstream.onComplete();
                    }
                });
            } catch (Exception e) {
                // TODO: global error handler for undeliverable errors
            }
        }

        @Override
        public void dispose() {
            if (!disposed.get() && disposed.compareAndSet(false, true)) {
                Disposable d = upstream.get();
                if (d != null) {
                    d.dispose();
                }
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed.get();
        }
    }
}
