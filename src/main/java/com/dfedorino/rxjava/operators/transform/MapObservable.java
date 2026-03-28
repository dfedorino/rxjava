package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;

import java.util.function.Function;

/**
 * Observable, который преобразует каждый элемент исходного потока
 * с помощью заданной функции mapper.
 *
 * @param <T> тип элементов исходного Observable
 * @param <R> тип элементов результирующего Observable
 */
public final class MapObservable<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<? super T, ? extends R> mapper;

    /**
     * Создаёт MapObservable с заданной функцией преобразования.
     *
     * @param source исходный Observable
     * @param mapper функция для преобразования элементов
     */
    public MapObservable(Observable<T> source, Function<? super T, ? extends R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(Observer<R> observer) {
        source.subscribe(new MapObserver<>(observer, mapper));
    }
}
