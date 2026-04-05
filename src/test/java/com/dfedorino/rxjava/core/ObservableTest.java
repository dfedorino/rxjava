package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ObservableTest {

    @Test
    @DisplayName("испускает элементы через onNext")
    void shouldEmitItems() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onNext(3); emitter.onComplete();
        }).subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(3).containsExactly(1, 2, 3);
    }

    @Test
    @DisplayName("испускает ошибку через onError")
    void shouldEmitError() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean errorReceived = new AtomicBoolean();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onError(testError);
        }).subscribe(TestObserver.<Integer>builder()
                .onNextAction(received::add)
                .onErrorAction(t -> { errorReceived.set(true); capturedError.set(t); })
                .build());

        assertThat(received).hasSize(1).containsExactly(1);
        assertThat(errorReceived.get()).isTrue();
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("испускает уведомление о завершении")
    void shouldEmitCompletion() {
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onComplete();
        }).subscribe(TestObserver.<Integer>builder()
                .onCompleteAction(() -> completed.set(true))
                .build());

        assertThat(completed.get()).isTrue();
    }

    @Test
    @DisplayName("останавливает эмиссию после dispose")
    void shouldStopAfterDispose() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            emitterRef.set(emitter);
            emitter.onNext(1); emitter.onNext(2); emitter.onNext(3); emitter.onNext(4);
        }).subscribe(TestObserver.<Integer>builder()
                .onNextAction(item -> {
                    received.add(item);
                    if (item == 2) emitterRef.get().dispose();
                })
                .build());

        assertThat(received).hasSize(2).containsExactly(1, 2);
    }

    @Test
    @DisplayName("возвращает true для isDisposed после dispose")
    void shouldReturnTrueForIsDisposed() {
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder().build());

        assertThat(emitterRef.get().isDisposed()).isFalse();
        emitterRef.get().dispose();
        assertThat(emitterRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("обрабатывает исключение в subscribe")
    void shouldHandleExceptionInSubscribe() {
        AtomicBoolean errorReceived = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
            throw new RuntimeException("Exception in subscribe");
        }).subscribe(TestObserver.<Integer>builder()
                .onErrorAction(t -> errorReceived.set(true))
                .build());

        assertThat(errorReceived.get()).isTrue();
    }

    @Test
    @DisplayName("останавливает эмиссию после onError")
    void shouldStopAfterOnError() {
        List<String> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onError(new RuntimeException("Test error"));
            emitter.onNext(2); emitter.onComplete();
        }).subscribe(TestObserver.<Integer>builder()
                .onNextAction(item -> received.add("onNext:" + item))
                .onErrorAction(t -> received.add("onError:" + t.getMessage()))
                .build());

        assertThat(received).hasSize(2).containsExactly("onNext:1", "onError:Test error");
    }

    @Test
    @DisplayName("останавливает эмиссию после onComplete")
    void shouldStopAfterOnComplete() {
        List<String> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onComplete();
            emitter.onNext(2); emitter.onError(new RuntimeException("Test error"));
        }).subscribe(TestObserver.<Integer>builder()
                .onNextAction(item -> received.add("onNext:" + item))
                .onCompleteAction(() -> received.add("onComplete"))
                .build());

        assertThat(received).hasSize(2).containsExactly("onNext:1", "onComplete");
    }
}
