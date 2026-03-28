package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.disposable.Disposable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Реализация ObservableEmitter для Observable.create().
 * Обеспечивает потокобезопасную отправку элементов и управление подпиской.
 *
 * @param <T> тип элементов, испускаемых этим эмиттером
 */
final class CreateEmitter<T> extends AtomicBoolean implements ObservableEmitter<T>, Disposable {

    private final Observer<T> observer;

    CreateEmitter(Observer<T> observer) {
        this.observer = observer;
    }

    @Override
    public void onNext(T value) {
        if (!get()) {
            observer.onNext(value);
        }
    }

    @Override
    public void onError(Throwable error) {
        if (compareAndSet(false, true)) {
            observer.onError(error);
        }
    }

    @Override
    public void onComplete() {
        if (compareAndSet(false, true)) {
            observer.onComplete();
        }
    }

    @Override
    public void dispose() {
        set(true);
    }

    @Override
    public boolean isDisposed() {
        return get();
    }
}
