package com.dfedorino.rxjava.operators.predicate;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;

import java.util.function.Predicate;

public final class FilterObservable<T> extends Observable<T> {

    private final Observable<T> source;
    private final Predicate<? super T> predicate;

    public FilterObservable(Observable<T> source, Predicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected void subscribeActual(Observer<T> observer) {
        source.subscribe(new FilterObserver<>(observer, predicate));
    }
}
