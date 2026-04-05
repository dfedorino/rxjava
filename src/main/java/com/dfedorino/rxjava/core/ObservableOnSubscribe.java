package com.dfedorino.rxjava.core;

@FunctionalInterface
public interface ObservableOnSubscribe<T> {
    void subscribe(ObservableEmitter<T> emitter) throws Throwable;
}
