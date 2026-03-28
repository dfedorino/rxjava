package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observer;

/**
 * Внутренний Observer для подписки на каждый созданный Observable.
 * Передаёт результаты в общий FlatMapObserver.
 *
 * @param <R> тип элементов потока
 */
public final class FlatMapInnerObserver<R> implements Observer<R>, Disposable {

    private final FlatMapObserver<?, R> parent;
    private Disposable disposable;

    /**
     * Создаёт FlatMapInnerObserver для подписки на внутренний Observable.
     *
     * @param parent родительский FlatMapObserver для координации
     */
    public FlatMapInnerObserver(FlatMapObserver<?, R> parent) {
        this.parent = parent;
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.disposable = d;
    }

    @Override
    public void onNext(R item) {
        if (parent.isDisposed()) {
            return;
        }
        parent.downstream.onNext(item);
    }

    @Override
    public void onError(Throwable t) {
        if (parent.isDisposed()) {
            return;
        }
        parent.innerError(t);
    }

    @Override
    public void onComplete() {
        if (parent.isDisposed()) {
            return;
        }
        parent.innerComplete();
    }

    @Override
    public void dispose() {
        if (disposable != null) {
            disposable.dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposable != null && disposable.isDisposed();
    }
}
