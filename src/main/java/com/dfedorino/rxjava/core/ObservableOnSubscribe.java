package com.dfedorino.rxjava.core;

/**
 * Интерфейс для пользовательской логики подписки Observable.
 * Реализация этого интерфейса определяет поведение Observable при подписке.
 *
 * @param <T> тип элементов, испускаемых этим Observable
 */
@FunctionalInterface
public interface ObservableOnSubscribe<T> {

    /**
     * Вызывается при подписке Observer на Observable.
     *
     * @param emitter эмиттер для отправки элементов и уведомлений Observer
     * @throws Throwable исключения, возникающие при подписке
     */
    void subscribe(ObservableEmitter<T> emitter) throws Throwable;
}
