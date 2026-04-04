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
 * Observer-обёртка, который управляет подписками на внутренние Observable
 * и координирует передачу результатов downstream.
 *
 * @param <T> тип элементов исходного потока
 * @param <R> тип элементов результирующего потока
 */
public final class FlatMapObserver<T, R> implements Observer<T>, Disposable {

    final Observer<R> downstream;
    private final Function<? super T, ? extends Observable<? extends R>> mapper;

    private volatile Disposable upstreamDisposable;
    private final AtomicInteger activeSubscriptions;
    private final AtomicBoolean isDisposed;
    private final AtomicBoolean sourceCompleted;
    private final Queue<Disposable> innerDisposables = new ConcurrentLinkedQueue<>();

    /**
     * Создаёт FlatMapObserver для управления слиянием потоков.
     *
     * @param downstream downstream Observer для получения результатов
     * @param mapper     функция для преобразования каждого элемента в Observable
     */
    public FlatMapObserver(Observer<R> downstream, Function<? super T, ? extends Observable<? extends R>> mapper) {
        this.downstream = downstream;
        this.mapper = mapper;
        this.activeSubscriptions = new AtomicInteger(0);
        this.isDisposed = new AtomicBoolean(false);
        this.sourceCompleted = new AtomicBoolean(false);
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.upstreamDisposable = d;
        downstream.onSubscribe(this);
    }

    @Override
    public void onNext(T item) {
        if (isDisposed.get()) {
            return;
        }

        try {
            Observable<? extends R> innerObservable = mapper.apply(item);
            activeSubscriptions.incrementAndGet();
            subscribeInner(innerObservable);
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
        if (isDisposed.getAndSet(true)) {
            return;
        }
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
        if (isDisposed.getAndSet(true)) {
            return;
        }
        if (upstreamDisposable != null) {
            upstreamDisposable.dispose();
        }
        disposeAllInnerSubscriptions();
    }

    @Override
    public boolean isDisposed() {
        return isDisposed.get();
    }

    /**
     * Вызывается внутренним Observer при завершении.
     */
    void innerComplete() {
        if (activeSubscriptions.decrementAndGet() == 0) {
            if (sourceCompleted.get() && !isDisposed.get()) {
                isDisposed.set(true);
                downstream.onComplete();
            }
        }
    }

    /**
     * Вызывается внутренним Observer при ошибке.
     */
    void innerError(Throwable t) {
        onError(t);
    }

    private void checkTermination() {
        if (sourceCompleted.get() && activeSubscriptions.get() == 0 && !isDisposed.get()) {
            isDisposed.set(true);
            downstream.onComplete();
        }
    }

    private void disposeAllInnerSubscriptions() {
        Disposable inner;
        while ((inner = innerDisposables.poll()) != null) {
            inner.dispose();
        }
    }
}
