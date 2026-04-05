package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.*;
import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class MapOperatorTest {

    @Test
    @DisplayName("преобразует каждый элемент через mapper")
    void shouldTransformEachElement() {
        List<String> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 5; i++) emitter.onNext(i);
                    emitter.onComplete();
                }).map(i -> "Value-" + i)
                .subscribe(TestObserver.<String>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(5)
                .containsExactly("Value-1", "Value-2", "Value-3", "Value-4", "Value-5");
    }

    @Test
    @DisplayName("обрабатывает пустой поток")
    void shouldHandleEmptyStream() {
        List<String> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(ObservableEmitter::onComplete)
                .map(i -> "Value-" + i)
                .subscribe(TestObserver.<String>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(received).isEmpty();
        assertThat(completed.get()).isTrue();
    }

    @Test
    @DisplayName("пробрасывает ошибку от источника")
    void shouldPropagateUpstreamError() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                }).map(i -> "Value-" + i)
                .subscribe(TestObserver.<String>builder().onErrorAction(capturedError::set).build());

        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("испускает исключение mapper через onError")
    void shouldEmitMapperException() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).map(i -> {
            if (i == 2) throw new IllegalArgumentException("Hate evens!");
            return "Value-" + i;
        }).subscribe(TestObserver.<String>builder().onErrorAction(capturedError::set).build());

        assertThat(capturedError.get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Hate evens!");
    }

    @Test
    @DisplayName("останавливает эмиссию после dispose")
    void shouldStopAfterDispose() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitterRef.set(emitter);
                    for (int i = 1; i <= 10; i++) emitter.onNext(i);
                    emitter.onComplete();
                }).map(i -> i * 2)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            received.add(item);
                            if (item == 4) emitterRef.get().dispose();
                        })
                        .build());

        assertThat(received).hasSize(2).containsExactly(2, 4);
    }

    @Test
    @DisplayName("поддерживает цепочку из нескольких map")
    void shouldSupportChaining() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                }).map(i -> i * 2).map(i -> i + 10)
                .subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(3).containsExactly(12, 14, 16);
    }

    @Test
    @DisplayName("позволяет mapper возвращать null")
    void shouldAllowNullValues() {
        List<String> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                }).map(i -> i == 1 ? null : "Value-" + i)
                .subscribe(TestObserver.<String>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(2);
        assertThat(received.getFirst()).isNull();
        assertThat(received.get(1)).isEqualTo("Value-2");
    }

    @Test
    @DisplayName("не испускает элементы после ошибки mapper")
    void shouldNotEmitAfterMapperError() {
        List<String> received = new ArrayList<>();
        AtomicInteger emitCount = new AtomicInteger();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onComplete();
                })
                .map(i -> {
                    emitCount.incrementAndGet();
                    if (i == 3) throw new IllegalStateException("Error at 3");
                    return "Value-" + i;
                })
                .subscribe(TestObserver.<String>builder()
                        .onNextAction(received::add)
                        .build());

        assertThat(received)
                .hasSize(2)
                .containsExactly("Value-1", "Value-2");
        assertThat(emitCount.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("обрабатывает identity mapper")
    void shouldHandleMapperReturningSameValue() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .map(i -> i)
                .subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(3).containsExactly(1, 2, 3);
    }
}
