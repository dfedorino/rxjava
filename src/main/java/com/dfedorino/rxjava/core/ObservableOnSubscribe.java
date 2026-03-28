package com.dfedorino.rxjava.core;

/**
 * Реализация этого интерфейса определяет поведение Observable при подписке.
 */
@FunctionalInterface
public interface ObservableOnSubscribe<T> {

    void subscribe(ObservableEmitter<T> emitter) throws Throwable;
}
