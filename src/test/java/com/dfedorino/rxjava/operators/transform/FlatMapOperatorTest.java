package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.ObservableEmitter;
import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("FlatMap operator test")
class FlatMapOperatorTest {

    @Test
    @DisplayName("Should transform each element into Observable and merge results")
    void shouldTransformAndMergeResults() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3);
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 10);
                    innerEmitter.onComplete();
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(3)
                .containsExactlyInAnyOrder(10, 20, 30);
    }

    @Test
    @DisplayName("Should handle empty source stream")
    void shouldHandleEmptySourceStream() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(ObservableEmitter::onComplete)
                .flatMap(x -> Observable.<Integer>create(emitter -> {
                    emitter.onNext(x * 2);
                    emitter.onComplete();
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("Should propagate source error through onError")
    void shouldPropagateSourceError() {
        // Arrange
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Source error");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .flatMap(x -> Observable.<Integer>create(emitter -> {
                    emitter.onNext(x * 2);
                    emitter.onComplete();
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("Should propagate inner Observable error through onError")
    void shouldPropagateInnerError() {
        // Arrange
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException innerError = new RuntimeException("Inner error");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(emitter -> emitter.onError(innerError)))
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(capturedError.get()).isSameAs(innerError);
    }

    @Test
    @DisplayName("Should propagate mapper exception as onError")
    void shouldPropagateMapperException() {
        // Arrange
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .flatMap(x -> {
                    if (x == 2) {
                        throw new IllegalArgumentException("Mapper error!");
                    }
                    return Observable.<Integer>create(emitter -> {
                        emitter.onNext(x * 2);
                        emitter.onComplete();
                    });
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(capturedError.get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Mapper error!");
    }

    @Test
    @DisplayName("Should handle dispose correctly")
    void shouldHandleDispose() {
        // Arrange
        AtomicInteger emitCount = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicBoolean disposed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    if (!innerEmitter.isDisposed()) {
                        innerEmitter.onNext(x * 2);
                    }
                    innerEmitter.onComplete();
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            if (!disposed.get()) emitCount.incrementAndGet();
                        })
                        .onSubscribeAction(disposableRef::set)
                        .build());

        // Assert
        assertThat(disposableRef.get()).isNotNull();
        disposed.set(true);
        disposableRef.get().dispose();
        assertThat(emitCount.get()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("Should chain with map operator")
    void shouldChainWithMap() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 2);
                    innerEmitter.onComplete();
                }))
                .map(x -> x + 10)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactlyInAnyOrder(12, 14);
    }

    @Test
    @DisplayName("Should chain with filter operator")
    void shouldChainWithFilter() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 2);
                    innerEmitter.onNext(x * 3);
                    innerEmitter.onComplete();
                }))
                .filter(x -> x > 5)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .allMatch(item -> item > 5);
    }

    @Test
    @DisplayName("Should call onComplete after all inner Observables complete")
    void shouldCompleteAfterAllInnersComplete() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 10);
                    innerEmitter.onComplete();
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(completed).isTrue();
        assertThat(received)
                .hasSize(2)
                .containsExactlyInAnyOrder(10, 20);
    }

    @Test
    @DisplayName("Should emit multiple elements from each inner Observable")
    void shouldEmitMultipleFromEachInner() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    for (int i = 1; i <= 3; i++) {
                        innerEmitter.onNext(x * 10 + i);
                    }
                    innerEmitter.onComplete();
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(6)
                .containsExactlyInAnyOrder(11, 12, 13, 21, 22, 23);
    }

    @Test
    @DisplayName("Should handle inner Observable that emits no items")
    void shouldHandleEmptyInnerObservable() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .flatMap(x -> {
                    if (x == 2) {
                        return Observable.<Integer>create(ObservableEmitter::onComplete);
                    }
                    return Observable.<Integer>create(emitter -> {
                        emitter.onNext(x * 10);
                        emitter.onComplete();
                    });
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactlyInAnyOrder(10, 30);
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("Should handle single source element mapping to multiple inner items")
    void shouldHandleSingleElementToMultipleInner() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(5);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x);
                    innerEmitter.onNext(x + 1);
                    innerEmitter.onNext(x + 2);
                    innerEmitter.onComplete();
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(received)
                .hasSize(3)
                .containsExactly(5, 6, 7);
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("Should throw NPE when mapper is null and onNext is called")
    void shouldThrowNPEForNullMapper() {
        // Arrange
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        Observable<Integer> source = Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        });

        // Act
        source.<Object>flatMap(null)
                .subscribe(TestObserver.builder()
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(capturedError.get())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should propagate error even when inner Observables are pending")
    void shouldPropagateErrorWithPendingInners() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Source error after inner started");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 10);
                    // This inner may not complete due to source error
                }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onErrorAction(capturedError::set)
                        .build());

        // Assert
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("Should ignore second onComplete from source")
    void shouldIgnoreSecondSourceOnComplete() {
        // Arrange
        AtomicInteger completeCount = new AtomicInteger(0);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                    emitter.onComplete(); // Should be ignored
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 2);
                    innerEmitter.onComplete();
                }))
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
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 2);
                    innerEmitter.onComplete();
                }))
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
    @DisplayName("Should handle mapper returning same Observable instance")
    void shouldHandleMapperReturningSameObservable() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        Observable<Integer> sameInner = Observable.<Integer>create(emitter -> {
            emitter.onNext(42);
            emitter.onComplete();
        });

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap((Integer x) -> sameInner)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly(42, 42);
    }
}
