package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Управляет подписками на внутренние Observable и координирует передачу результатов
 */
public final class FlatMapObserver<T, R> implements Observer<T>, Disposable {
    final Observer<R> downstream;
    private final Function<? super T, ? extends Observable<? extends R>> mapper;
    private volatile Disposable upstreamDisposable;
    private final AtomicInteger activeSubscriptions = new AtomicInteger();
    private final AtomicBoolean isDisposed = new AtomicBoolean();
    private final AtomicBoolean sourceCompleted = new AtomicBoolean();
    private final Queue<Disposable> innerDisposables = new ConcurrentLinkedQueue<>();

    public FlatMapObserver(Observer<R> downstream, Function<? super T, ? extends Observable<? extends R>> mapper) {
        this.downstream = downstream;
        this.mapper = mapper;
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.upstreamDisposable = d;
        downstream.onSubscribe(this);
    }

    @Override
    public void onNext(T item) {
        activeSubscriptions.incrementAndGet();
        if (isDisposed.get()) {
            activeSubscriptions.decrementAndGet();
            checkTermination();
            return;
        }
        try {
            Observable<? extends R> innerObs = mapper.apply(item);
            subscribeInner(innerObs);
        } catch (Throwable e) {
            activeSubscriptions.decrementAndGet();
            onError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void subscribeInner(Observable<? extends R> innerObservable) {
        FlatMapInnerObserver<R> innerObserver = new FlatMapInnerObserver<>(this);
        innerDisposables.add(innerObserver);
        ((Observable<R>) innerObservable).subscribe(innerObserver);
    }

    @Override
    public void onError(Throwable t) {
        if (isDisposed.getAndSet(true)) return;
        upstreamDisposable.dispose();
        disposeAllInnerSubscriptions();
        downstream.onError(t);
    }

    @Override
    public void onComplete() {
        sourceCompleted.set(true);
        checkTermination();
    }

    @Override
    public void dispose() {
        if (isDisposed.getAndSet(true)) return;
        if (upstreamDisposable != null) upstreamDisposable.dispose();
        disposeAllInnerSubscriptions();
    }

    @Override
    public boolean isDisposed() {
        return isDisposed.get();
    }

    void innerComplete() {
        if (activeSubscriptions.decrementAndGet() == 0 && sourceCompleted.get()) {
            tryTerminate();
        }
    }

    void innerError(Throwable t) {
        onError(t);
    }

    private void checkTermination() {
        if (sourceCompleted.get() && activeSubscriptions.get() == 0) {
            tryTerminate();
        }
    }

    private void tryTerminate() {
        if (isDisposed.getAndSet(true)) return;
        downstream.onComplete();
    }

    private void disposeAllInnerSubscriptions() {
        Disposable inner;
        while ((inner = innerDisposables.poll()) != null) {
            inner.dispose();
        }
    }
}
