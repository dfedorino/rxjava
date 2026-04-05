package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;

import java.util.function.Function;

public final class FlatMapObservable<T, R> extends Observable<R> {
    private final Observable<T> source;
    private final Function<? super T, ? extends Observable<? extends R>> mapper;

    public FlatMapObservable(Observable<T> source, Function<? super T, ? extends Observable<? extends R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(Observer<R> observer) {
        source.subscribe(new FlatMapObserver<>(observer, mapper));
    }
}
