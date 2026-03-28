package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.operators.predicate.FilterObservable;
import com.dfedorino.rxjava.operators.transform.MapObservable;

import java.util.function.Function;
import java.util.function.Predicate;

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

    /**
     * Преобразует каждый элемент потока с помощью заданной функции mapper.
     *
     * @param mapper функция для преобразования элементов
     * @param <R> тип элементов результирующего Observable
     * @return новый Observable с преобразованными элементами
     */
    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        return new MapObservable<>(this, mapper);
    }

    /**
     * Пропускает только элементы, удовлетворяющие заданному предикату.
     *
     * @param predicate предикат для фильтрации элементов
     * @return новый Observable с отфильтрованными элементами
     */
    public Observable<T> filter(Predicate<? super T> predicate) {
        return new FilterObservable<>(this, predicate);
    }
}
