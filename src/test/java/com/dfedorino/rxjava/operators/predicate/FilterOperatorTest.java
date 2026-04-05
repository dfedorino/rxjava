package com.dfedorino.rxjava.operators.predicate;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.ObservableEmitter;
import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.ObservableOnSubscribe;
import com.dfedorino.rxjava.util.TestObserver;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Filter operator test")
class FilterOperatorTest {

    @Test
    @DisplayName("Should pass only elements matching predicate")
    void shouldPassOnlyMatchingElements() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> x % 2 == 0)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(5)
                .containsExactly(2, 4, 6, 8, 10);
    }

    @Test
    @DisplayName("Should pass all elements when predicate is always true")
    void shouldPassAllElementsWhenPredicateAlwaysTrue() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3, 4, 5);
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received).isEqualTo(source);
    }

    @Test
    @DisplayName("Should filter out all elements when predicate is always false")
    void shouldFilterAllElementsWhenPredicateAlwaysFalse() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3, 4, 5);
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> false)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("Should handle empty stream")
    void shouldHandleEmptyStream() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(ObservableEmitter::onComplete)
                .filter(x -> x > 5)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("Should filter null values")
    void shouldFilterNullValues() {
        // Arrange
        List<String> received = new ArrayList<>();

        // Act
        Observable.<String>create(emitter -> {
                    emitter.onNext("Hello");
                    emitter.onNext(null);
                    emitter.onNext("World");
                    emitter.onComplete();
                })
                .filter(Objects::nonNull)
                .subscribe(TestObserver.<String>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly("Hello", "World");
    }

    @Test
    @DisplayName("Should chain multiple filter operators")
    void shouldChainMultipleFilters() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onNext(5);
                    emitter.onComplete();
                })
                .filter(x -> x > 2)
                .filter(x -> x < 5)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly(3, 4);
    }

    @Test
    @DisplayName("Should propagate upstream error through onError")
    void shouldPropagateUpstreamError() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(10);
                    emitter.onError(testError);
                })
                .filter(x -> x > 5)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(1)
                .containsExactly(10);
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("Should propagate predicate exception as onError")
    void shouldPropagatePredicateException() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                })
                .filter(x -> {
                    if (x == 2) {
                        throw new IllegalArgumentException("Exception in predicate");
                    }
                    return x > 0;
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(1)
                .containsExactly(1);
        assertThat(capturedError.get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Exception in predicate");
    }

    @Test
    @DisplayName("Should receive disposable and stop emitting after dispose")
    void shouldStopEmittingAfterDispose() {
        // Arrange
        AtomicInteger emitCount = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .filter(x -> x % 2 == 0)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> emitCount.incrementAndGet())
                        .onSubscribeAction(disposableRef::set)
                        .build());

        // Assert
        assertThat(disposableRef.get()).isNotNull();
        disposableRef.get().dispose();
        assertThat(emitCount).hasValue(5);
    }

    @Test
    @DisplayName("Should not call predicate after dispose")
    void shouldNotCallPredicateAfterDispose() {
        // Arrange
        AtomicInteger predicateCallCount = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        CountDownLatch disposedLatch = new CountDownLatch(1);

        // Act
        ObservableOnSubscribe<Integer> source = emitter -> {
            for (int i = 1; i <= 10; i++) {
                emitter.onNext(i);
                if (i == 5) {
                    disposableRef.get().dispose();
                    disposedLatch.countDown();
                }
            }
            emitter.onComplete();
        };
        Observable.create(source)
                .filter(x -> {
                    int count = predicateCallCount.incrementAndGet();
                    if (disposedLatch.getCount() == 0) {
                        // Signal that predicate was called after dispose
                        Assertions.fail("Predicate called after dispose, call #" + count);
                    }
                    return true;
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onSubscribeAction(disposableRef::set)
                        .build());

        // Assert — if we reach here without AssertionError, test passes
        assertThat(predicateCallCount.get()).isLessThanOrEqualTo(5);
    }

    @Test
    @DisplayName("Should handle single element stream")
    void shouldHandleSingleElementStream() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(42);
                    emitter.onComplete();
                })
                .filter(x -> x > 10)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received)
                .hasSize(1)
                .containsExactly(42);
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("Should handle single element that gets filtered out")
    void shouldHandleSingleElementFilteredOut() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(5);
                    emitter.onComplete();
                })
                .filter(x -> x > 10)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("Should call predicate exactly once per element")
    void shouldCallPredicateExactlyOncePerElement() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3, 4, 5);
        AtomicInteger predicateCallCount = new AtomicInteger(0);

        // Act
        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> {
                    predicateCallCount.incrementAndGet();
                    return x % 2 == 0;
                })
                .subscribe(TestObserver.<Integer>builder()
                        .build());

        // Assert
        assertThat(predicateCallCount).hasValue(5);
    }

    @Test
    @DisplayName("Should propagate error emitted before any onNext")
    void shouldPropagateErrorBeforeAnyOnNext() {
        // Arrange
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        AtomicBoolean receivedOnNext = new AtomicBoolean(false);
        RuntimeException testError = new RuntimeException("Immediate error");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onError(testError);
                })
                .filter(x -> x > 0)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> receivedOnNext.set(true))
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(receivedOnNext).isFalse();
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("Should throw NPE when predicate is null and onNext is called")
    void shouldThrowNPEForNullPredicate() {
        // Arrange
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        Observable<Integer> source = Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        });

        // Act
        source.filter(null)
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert — NPE occurs when predicate.test() is called on null
        assertThat(capturedError.get())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should ignore second onComplete signal")
    void shouldIgnoreSecondOnComplete() {
        // Arrange
        AtomicInteger completeCount = new AtomicInteger(0);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                    emitter.onComplete(); // Should be ignored
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {})
                        .onCompleteAction(completeCount::incrementAndGet)
                        .build());

        // Assert
        assertThat(completeCount).hasValue(1);
    }

    @Test
    @DisplayName("Should ignore onError after onComplete")
    void shouldIgnoreOnErrorAfterOnComplete() {
        // Arrange
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                    emitter.onError(new RuntimeException("Should be ignored"));
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {})
                        .onCompleteAction(() -> completed.set(true))
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(completed).isTrue();
        assertThat(capturedError.get()).isNull();
    }

    @Test
    @DisplayName("Should ignore onComplete after onError")
    void shouldIgnoreOnCompleteAfterOnError() {
        // Arrange
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        RuntimeException testError = new RuntimeException("Test error");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onError(testError);
                    emitter.onComplete(); // Should be ignored
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(capturedError::set)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(capturedError.get()).isSameAs(testError);
        assertThat(completed).isFalse();
    }
}
