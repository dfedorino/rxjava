package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observer;

public final class FlatMapInnerObserver<T> implements Observer<T>, Disposable {
    private final FlatMapObserver<?, T> parent;
    private volatile Disposable disposable;

    public FlatMapInnerObserver(FlatMapObserver<?, T> parent) {
        this.parent = parent;
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.disposable = d;
    }

    @Override
    public void onNext(T item) {
        if (!parent.isDisposed()) {
            parent.downstream.onNext(item);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!parent.isDisposed()) {
            parent.innerError(t);
        }
    }

    @Override
    public void onComplete() {
        if (!parent.isDisposed()) {
            parent.innerComplete();
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
