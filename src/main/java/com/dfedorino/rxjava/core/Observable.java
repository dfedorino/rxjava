package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.operators.predicate.FilterObservable;
import com.dfedorino.rxjava.operators.threading.ObserveOnObservable;
import com.dfedorino.rxjava.operators.threading.SubscribeOnObservable;
import com.dfedorino.rxjava.operators.transform.FlatMapObservable;
import com.dfedorino.rxjava.operators.transform.MapObservable;
import com.dfedorino.rxjava.scheduler.Scheduler;

import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Observable<T> {
    public void subscribe(Observer<T> observer) {
        subscribeActual(observer);
    }

    protected abstract void subscribeActual(Observer<T> observer);

    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        if (source == null) {
            return new Observable<>() {
                @Override
                protected void subscribeActual(Observer<T> observer) {
                    observer.onSubscribe(new Disposable() {
                        @Override
                        public void dispose() {
                        }

                        @Override
                        public boolean isDisposed() {
                            return true;
                        }
                    });
                    observer.onError(new NullPointerException("source is null"));
                }
            };
        }
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

    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        return new MapObservable<>(this, mapper);
    }

    public Observable<T> filter(Predicate<? super T> predicate) {
        return new FilterObservable<>(this, predicate);
    }

    public <R> Observable<R> flatMap(Function<? super T, ? extends Observable<? extends R>> mapper) {
        return new FlatMapObservable<>(this, mapper);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new SubscribeOnObservable<>(this, scheduler);
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new ObserveOnObservable<>(this, scheduler);
    }
}
