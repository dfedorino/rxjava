package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observer;

import java.util.function.Function;

/**
 * Observer-обёртка, который преобразует элементы перед передачей
 * downstream Observer с помощью функции mapper.
 *
 * @param <T> тип элементов исходного потока
 * @param <R> тип элементов после преобразования
 */
public final class MapObserver<T, R> implements Observer<T>, Disposable {

    private final Observer<R> downstream;
    private final Function<? super T, ? extends R> mapper;
    private Disposable disposable;

    /**
     * Создаёт MapObserver для преобразования элементов.
     *
     * @param downstream downstream Observer для получения преобразованных элементов
     * @param mapper     функция для преобразования элементов
     */
    public MapObserver(Observer<R> downstream, Function<? super T, ? extends R> mapper) {
        this.downstream = downstream;
        this.mapper = mapper;
    }

    @Override
    public void onSubscribe(Disposable d) {
        this.disposable = d;
        downstream.onSubscribe(d);
    }

    @Override
    public void onNext(T item) {
        try {
            R result = mapper.apply(item);
            downstream.onNext(result);
        } catch (Throwable e) {
            onError(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        downstream.onError(t);
    }

    @Override
    public void onComplete() {
        downstream.onComplete();
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
