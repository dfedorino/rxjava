package com.dfedorino.rxjava.core;

public interface ObservableEmitter<T> extends Disposable {

    void onNext(T value);

    void onError(Throwable error);

    void onComplete();
}
