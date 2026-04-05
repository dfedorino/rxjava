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

@DisplayName("Map operator")
class MapOperatorTest {

    @Test
    @DisplayName("should transform each element via mapper function")
    void shouldTransformEachElement() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3, 4, 5);
        List<String> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .map(i -> "Value-" + i)
                .subscribe(TestObserver.<String>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(5)
                .containsExactly("Value-1", "Value-2", "Value-3", "Value-4", "Value-5");
    }

    @Test
    @DisplayName("should handle empty source stream")
    void shouldHandleEmptyStream() {
        // Arrange
        List<String> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(ObservableEmitter::onComplete)
                .map(i -> "Value-" + i)
                .subscribe(TestObserver.<String>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received).isEmpty();
        assertThat(completed.get()).isTrue();
    }

    @Test
    @DisplayName("should propagate upstream error via onError")
    void shouldPropagateUpstreamError() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .map(i -> "Value-" + i)
                .subscribe(TestObserver.<String>builder()
                        .onErrorAction(t -> {
                            errorReceived.set(true);
                            capturedError.set(t);
                        })
                        .build());

        // Assert
        assertThat(errorReceived.get()).isTrue();
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("should emit mapper exception via onError")
    void shouldEmitMapperExceptionViaOnError() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .map(i -> {
                    if (i == 2) {
                        throw new IllegalArgumentException("Hate evens!");
                    }
                    return "Value-" + i;
                })
                .subscribe(TestObserver.<String>builder()
                        .onErrorAction(t -> {
                            errorReceived.set(true);
                            capturedError.set(t);
                        })
                        .build());

        // Assert
        assertThat(errorReceived.get()).isTrue();
        assertThat(capturedError.get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Hate evens!");
    }

    @Test
    @DisplayName("should stop emission after dispose")
    void shouldStopEmissionAfterDispose() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitterRef.set(emitter);
                    for (int i = 1; i <= 10; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .map(i -> i * 2)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            received.add(item);
                            if (item == 4) { // after receiving source item 2
                                emitterRef.get().dispose();
                            }
                        })
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly(2, 4);
    }

    @Test
    @DisplayName("should support chaining multiple map operators")
    void shouldSupportChainingMultipleMapOperators() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .map(i -> i * 2)
                .map(i -> i + 10)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(3)
                .containsExactly(12, 14, 16);
    }

    @Test
    @DisplayName("should allow mapper to return null values")
    void shouldAllowMapperToReturnNullValues() {
        // Arrange
        List<String> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .map(i -> i == 1 ? null : "Value-" + i)
                .subscribe(TestObserver.<String>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received).hasSize(2);
        assertThat(received.get(0)).isNull();
        assertThat(received.get(1)).isEqualTo("Value-2");
    }
}
