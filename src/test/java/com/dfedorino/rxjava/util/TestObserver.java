package com.dfedorino.rxjava.util;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observer;

import java.util.function.Consumer;

/**
 * Reusable test Observer that delegates callbacks to provided actions.
 *
 * @param <T> type of elements
 */
public final class TestObserver<T> implements Observer<T> {

    private final Consumer<T> onNextAction;
    private final Consumer<Throwable> onErrorAction;
    private final Runnable onCompleteAction;
    private final Consumer<Disposable> onSubscribeAction;

    public TestObserver(Consumer<T> onNextAction,
                        Consumer<Throwable> onErrorAction,
                        Runnable onCompleteAction,
                        Consumer<Disposable> onSubscribeAction) {
        this.onNextAction = onNextAction;
        this.onErrorAction = onErrorAction != null ? onErrorAction : t -> {};
        this.onCompleteAction = onCompleteAction != null ? onCompleteAction : () -> {};
        this.onSubscribeAction = onSubscribeAction != null ? onSubscribeAction : d -> {};
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public void onSubscribe(Disposable d) {
        onSubscribeAction.accept(d);
    }

    @Override
    public void onNext(T item) {
        if (onNextAction != null) {
            try {
                onNextAction.accept(item);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        onErrorAction.accept(t);
    }

    @Override
    public void onComplete() {
        onCompleteAction.run();
    }

    public static class Builder<T> {
        private Consumer<T> onNextAction;
        private Consumer<Throwable> onErrorAction;
        private Runnable onCompleteAction;
        private Consumer<Disposable> onSubscribeAction;

        public Builder<T> onNextAction(Consumer<T> onNextAction) {
            this.onNextAction = onNextAction;
            return this;
        }

        public Builder<T> onErrorAction(Consumer<Throwable> onErrorAction) {
            this.onErrorAction = onErrorAction;
            return this;
        }

        public Builder<T> onCompleteAction(Runnable onCompleteAction) {
            this.onCompleteAction = onCompleteAction;
            return this;
        }

        public Builder<T> onSubscribeAction(Consumer<Disposable> onSubscribeAction) {
            this.onSubscribeAction = onSubscribeAction;
            return this;
        }

        public TestObserver<T> build() {
            return new TestObserver<>(onNextAction, onErrorAction, onCompleteAction, onSubscribeAction);
        }
    }
}
