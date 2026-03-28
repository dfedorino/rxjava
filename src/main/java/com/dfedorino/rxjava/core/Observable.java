package com.dfedorino.rxjava.core;

public abstract class Observable<T> {

    public void subscribe(Observer<T> observer) {
        subscribeActual(observer);
    }

    protected abstract void subscribeActual(Observer<T> observer);

    /**
     * Создаёт Observable с пользовательской логикой подписки.
     *
     * @param source источник с логикой подписки
     * @param <T> тип элементов
     * @return новый Observable
     */
    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        return new Observable<T>() {
            @Override
            protected void subscribeActual(Observer<T> observer) {
                CreateEmitter<T> emitter = new CreateEmitter<>(observer);
                observer.onSubscribe(emitter);
                try {
                    source.subscribe(emitter);
                } catch (Throwable e) {
                    emitter.onError(e);
                }
            }
        };
    }
}
