package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Observable")
class ObservableTest {

    @Test
    @DisplayName("should emit items via onNext")
    void shouldEmitItemsViaOnNext() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Assert
        assertThat(received)
                .hasSize(3)
                .containsExactly(1, 2, 3);
    }

    @Test
    @DisplayName("should emit error via onError")
    void shouldEmitErrorViaOnError() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onErrorAction(t -> {
                            errorReceived.set(true);
                            capturedError.set(t);
                        })
                        .build());

        // Assert
        assertThat(received).hasSize(1).containsExactly(1);
        assertThat(errorReceived.get()).isTrue();
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("should emit completion notification")
    void shouldEmitCompletionNotification() {
        // Arrange
        AtomicBoolean completed = new AtomicBoolean(false);

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        // Assert
        assertThat(completed.get()).isTrue();
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
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            received.add(item);
                            if (item == 2) {
                                emitterRef.get().dispose();
                            }
                        })
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly(1, 2);
    }

    @Test
    @DisplayName("should return true for isDisposed after dispose")
    void shouldReturnTrueForIsDisposedAfterDispose() {
        // Arrange
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .build());

        // Act & Assert
        assertThat(emitterRef.get().isDisposed()).isFalse();

        emitterRef.get().dispose();

        assertThat(emitterRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("should handle exception thrown in subscribe")
    void shouldHandleExceptionThrownInSubscribe() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);

        // Act
        Observable.<Integer>create(emitter -> {
                    throw new RuntimeException("Exception in subscribe");
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(t -> errorReceived.set(true))
                        .build());

        // Assert
        assertThat(errorReceived.get()).isTrue();
    }

    @Test
    @DisplayName("should stop emission after onError")
    void shouldStopEmissionAfterOnError() {
        // Arrange
        List<String> received = new ArrayList<>();

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onError(new RuntimeException("Test error"));
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> received.add("onNext:" + item))
                        .onErrorAction(t -> received.add("onError:" + t.getMessage()))
                        .onCompleteAction(() -> received.add("onComplete"))
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly("onNext:1", "onError:Test error");
    }

    @Test
    @DisplayName("should stop emission after onComplete")
    void shouldStopEmissionAfterOnComplete() {
        // Arrange
        List<String> received = new ArrayList<>();

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                    emitter.onNext(2);
                    emitter.onError(new RuntimeException("Test error"));
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> received.add("onNext:" + item))
                        .onErrorAction(t -> received.add("onError:" + t.getMessage()))
                        .onCompleteAction(() -> received.add("onComplete"))
                        .build());

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly("onNext:1", "onComplete");
    }
}
