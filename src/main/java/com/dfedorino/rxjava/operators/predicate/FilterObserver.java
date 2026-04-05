package com.dfedorino.rxjava.operators.predicate;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.exception.ErrorHandlers;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

public final class FilterObserver<T> implements Observer<T>, Disposable {
    private final Observer<T> downstream;
    private final Predicate<? super T> predicate;
    private volatile Disposable disposable;
    private final AtomicBoolean terminated = new AtomicBoolean();

    public FilterObserver(Observer<T> downstream, Predicate<? super T> predicate) {
        this.downstream = downstream;
        this.predicate = predicate;
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.disposable = d;
        downstream.onSubscribe(d);
    }

    @Override
    public void onNext(T item) {
        if (terminated.get() || isDisposed()) return;
        try {
            if (predicate.test(item)) {
                downstream.onNext(item);
            }
        } catch (Throwable e) {
            onError(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (terminated.compareAndSet(false, true)) {
            downstream.onError(t);
        } else {
            ErrorHandlers.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (terminated.compareAndSet(false, true)) {
            downstream.onComplete();
        } else if (terminated.get()) {
            ErrorHandlers.onError(new IllegalStateException("onComplete called after termination"));
        }
    }

    @Override
    public void dispose() {
        if (disposable != null) disposable.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposable != null && disposable.isDisposed();
    }
}
