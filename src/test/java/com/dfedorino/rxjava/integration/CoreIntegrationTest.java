package com.dfedorino.rxjava.integration;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.ObservableEmitter;
import com.dfedorino.rxjava.scheduler.Schedulers;
import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CoreIntegrationTest {

    @Test
    @DisplayName("Complete flow: create -> subscribeOn -> flatMap -> observeOn -> map -> filter -> subscribe")
    void shouldCompleteFullPipelineWithAllOperators() throws InterruptedException {
        // Arrange
        List<String> received = new ArrayList<>();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onComplete();
                })
                .subscribeOn(Schedulers.computation())
                .flatMap(x -> Observable.<Integer>create(e -> {
                    e.onNext(x);
                    e.onNext(x * 10);
                    e.onComplete();
                }))
                .observeOn(Schedulers.io())
                .map(i -> "Value-" + i)
                .filter(s -> !s.contains("Value-2"))
                .subscribe(TestObserver.<String>builder()
                    .onSubscribeAction(disposableRef::set)
                    .onNextAction(received::add)
                    .onCompleteAction(() -> {
                        completed.set(true);
                        latch.countDown();
                    })
                    .onErrorAction(t -> latch.countDown())
                    .build());

        boolean completedInTime = latch.await(5, TimeUnit.SECONDS);

        // Assert
        assertThat(completedInTime)
            .as("Flow should complete within timeout")
            .isTrue();
        assertThat(disposableRef.get())
            .as("Disposable should be set")
            .isNotNull();
        assertThat(disposableRef.get().isDisposed())
            .as("Disposable should be disposed after completion")
            .isTrue();
        assertThat(received)
            .as("Should receive filtered and transformed values")
            .containsExactly("Value-1", "Value-10", "Value-3", "Value-30", "Value-4", "Value-40");
        assertThat(completed.get())
            .as("onComplete should be called")
            .isTrue();
    }

    @Test
    @DisplayName("Dispose stops emission and prevents further onNext calls")
    void shouldStopEmissionAfterDispose() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitterRef.set(emitter);
                    emitter.onNext(1);
                    emitter.onNext(2);
                    // Dispose after receiving 2
                    emitter.dispose();
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onComplete();
                })
                .subscribe(TestObserver.<Integer>builder()
                    .onNextAction(received::add)
                    .build());

        // Assert
        assertThat(received)
            .as("Should only receive values before dispose")
            .containsExactly(1, 2);
        assertThat(emitterRef.get().isDisposed())
            .as("Emitter should be disposed")
            .isTrue();
    }

    @Test
    @DisplayName("onError automatically disposes and prevents subsequent events")
    void shouldAutoDisposeOnError() {
        // Arrange
        List<String> received = new ArrayList<>();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                    // These should be blocked after error
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .subscribe(TestObserver.<Integer>builder()
                    .onSubscribeAction(disposableRef::set)
                    .onNextAction(item -> received.add("onNext:" + item))
                    .onErrorAction(t -> {
                        capturedError.set(t);
                        received.add("onError:" + t.getMessage());
                    })
                    .build());

        // Assert
        assertThat(received)
            .as("Should receive onNext then onError, but not subsequent events")
            .containsExactly("onNext:1", "onError:Test error");
        assertThat(capturedError.get())
            .as("Captured error should match original error")
            .isSameAs(testError);
        assertThat(disposableRef.get().isDisposed())
            .as("Disposable should be auto-disposed on error")
            .isTrue();
    }

    @Test
    @DisplayName("dispose() is idempotent - multiple calls are safe")
    void shouldHandleMultipleDisposeCalls() {
        // Arrange
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribe(TestObserver.<Integer>builder()
                    .onSubscribeAction(disposableRef::set)
                    .build());

        // Dispose multiple times
        disposableRef.get().dispose();
        disposableRef.get().dispose();
        disposableRef.get().dispose();

        // Assert
        assertThat(disposableRef.get().isDisposed())
            .as("dispose() should be idempotent")
            .isTrue();
    }

    @Test
    @DisplayName("onNext blocked after dispose - verifies atomic state check")
    void shouldBlockOnNextAfterDispose() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        // Act
        Observable.<Integer>create(emitter -> {
                    emitterRef.set(emitter);
                    emitter.onNext(1);
                    // Dispose immediately
                    emitter.dispose();
                    // These should be blocked
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .subscribe(TestObserver.<Integer>builder()
                    .onNextAction(received::add)
                    .build());

        // Assert
        assertThat(received)
            .as("Should only receive value before dispose")
            .containsExactly(1);
        assertThat(emitterRef.get().isDisposed())
            .as("Emitter should be disposed")
            .isTrue();
    }
}
