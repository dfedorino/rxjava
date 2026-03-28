package com.dfedorino.rxjava.core;

/**
 * Абстрактный класс, представляющий источник данных реактивного потока.
 * Observable испускает элементы, которые наблюдаются через Observer.
 *
 * @param <T> тип элементов, испускаемых этим Observable
 */
public abstract class Observable<T> {

    /**
     * Подписывает Observer на поток данных этого Observable.
     *
     * @param observer Observer, который будет получать элементы потока
     */
    public void subscribe(Observer<T> observer) {
        subscribeActual(observer);
    }

    /**
     * Абстрактный метод для фактической реализации подписки.
     * Вызывается методом subscribe().
     *
     * @param observer Observer, который будет получать элементы потока
     */
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
