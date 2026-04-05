package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.exception.ErrorHandlers;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class MapObserver<T, R> implements Observer<T>, Disposable {
    private final Observer<R> downstream;
    private final Function<? super T, ? extends R> mapper;
    private volatile Disposable disposable;
    private final AtomicBoolean terminated = new AtomicBoolean();

    public MapObserver(Observer<R> downstream, Function<? super T, ? extends R> mapper) {
        this.downstream = downstream;
        this.mapper = mapper;
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.disposable = d;
        downstream.onSubscribe(d);
    }

    @Override
    public void onNext(T item) {
        if (terminated.get()) return;
        try {
            downstream.onNext(mapper.apply(item));
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
