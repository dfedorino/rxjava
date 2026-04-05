package com.dfedorino.rxjava.operators.predicate;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Observer-обёртка, который проверяет предикат перед передачей
 * элемента downstream Observer.
 *
 * @param <T> тип элементов потока
 */
public final class FilterObserver<T> implements Observer<T>, Disposable {

    private final Observer<T> downstream;
    private final Predicate<? super T> predicate;
    private volatile Disposable disposable;
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    /**
     * Создаёт FilterObserver для фильтрации элементов.
     *
     * @param downstream downstream Observer для получения отфильтрованных элементов
     * @param predicate предикат для проверки элементов
     */
    public FilterObserver(Observer<T> downstream, Predicate<? super T> predicate) {
        this.downstream = downstream;
        this.predicate = predicate;
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.disposable = d;
        downstream.onSubscribe(d);
    }

    @Override
    public void onNext(T item) {
        if (terminated.get() || isDisposed()) {
            return;
        }

        try {
            if (predicate.test(item)) {
                downstream.onNext(item);
            }
            // Если предикат возвращает false, элемент игнорируется
        } catch (Throwable e) {
            onError(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (terminated.compareAndSet(false, true)) {
            downstream.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (terminated.compareAndSet(false, true)) {
            downstream.onComplete();
        }
    }

    @Override
    public void dispose() {
        if (disposable != null) {
            disposable.dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposable != null && disposable.isDisposed();
    }
}
