package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;

import java.util.function.Function;

public final class MapObservable<T, R> extends Observable<R> {
    private final Observable<T> source;
    private final Function<? super T, ? extends R> mapper;

    public MapObservable(Observable<T> source, Function<? super T, ? extends R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(Observer<R> observer) {
        source.subscribe(new MapObserver<>(observer, mapper));
    }
}
