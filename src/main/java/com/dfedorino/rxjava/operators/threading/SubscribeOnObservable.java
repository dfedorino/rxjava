package com.dfedorino.rxjava.operators.threading;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.scheduler.Scheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Observable-обёртка, выполняющая подписку на источник в указанном Scheduler.
 *
 * @param <T> тип элементов потока
 */
public class SubscribeOnObservable<T> extends Observable<T> {

    private final Observable<T> source;
    private final Scheduler scheduler;

    public SubscribeOnObservable(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(Observer<T> observer) {
        SubscribeOnObserver<T> parent = new SubscribeOnObserver<>(observer);
        observer.onSubscribe(parent);

        Runnable subscribeTask = () -> source.subscribe(parent);
        try {
            scheduler.execute(subscribeTask);
        } catch (Exception e) {
            parent.onError(e);
        }
    }

    /**
     * Observer-обёртка, которая сохраняет Disposable от источника
     * и передаёт его downstream.
     *
     * @param <T> тип элементов потока
     */
    static final class SubscribeOnObserver<T> implements Observer<T>, Disposable {

        private final Observer<T> downstream;
        private final AtomicReference<Disposable> upstream = new AtomicReference<>();
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        SubscribeOnObserver(Observer<T> downstream) {
            this.downstream = downstream;
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
            if (!disposed.get()) {
                downstream.onNext(item);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (!disposed.get()) {
                downstream.onError(t);
            } else {
                // TODO: global error handler for undeliverable errors
            }
        }

        @Override
        public void onComplete() {
            if (!disposed.get()) {
                disposed.set(true);
                downstream.onComplete();
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
