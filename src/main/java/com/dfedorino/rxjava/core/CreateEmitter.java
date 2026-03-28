package com.dfedorino.rxjava.core;

import java.util.concurrent.atomic.AtomicBoolean;

final class CreateEmitter<T> implements ObservableEmitter<T>, Disposable {

    private final Observer<T> observer;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    CreateEmitter(Observer<T> observer) {
        this.observer = observer;
    }

    @Override
    public void onNext(T value) {
        if (!terminated.get() && !disposed.get()) {
            observer.onNext(value);
        }
    }

    @Override
    public void onError(Throwable error) {
        if (terminated.compareAndSet(false, true)) {
            disposed.set(true); // onError автоматически вызывает dispose
            observer.onError(error);
        }
    }

    @Override
    public void onComplete() {
        if (terminated.compareAndSet(false, true)) {
            observer.onComplete();
        }
    }

    @Override
    public void dispose() {
        disposed.set(true);
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
