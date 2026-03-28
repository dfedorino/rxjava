package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;

import java.util.function.Function;

/**
 * Observable, который преобразует каждый элемент исходного потока
 * в новый Observable с помощью заданной функции mapper и сливает
 * результаты всех внутренних Observable в один выходной поток.
 *
 * @param <T> тип элементов исходного Observable
 * @param <R> тип элементов результирующего Observable
 */
public final class FlatMapObservable<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<? super T, ? extends Observable<? extends R>> mapper;

    /**
     * Создаёт FlatMapObservable с заданной функцией преобразования.
     *
     * @param source исходный Observable
     * @param mapper функция для преобразования каждого элемента в Observable
     */
    public FlatMapObservable(Observable<T> source, Function<? super T, ? extends Observable<? extends R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(Observer<R> observer) {
        source.subscribe(new FlatMapObserver<>(observer, mapper));
    }
}
