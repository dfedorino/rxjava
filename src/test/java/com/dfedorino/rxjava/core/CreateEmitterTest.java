package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

@DisplayName("CreateEmitter")
class CreateEmitterTest {

    @Test
    @DisplayName("should stop emitting after dispose")
    void shouldStopEmittingAfterDispose() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Act
        emitterRef.get().dispose();
        emitterRef.get().onNext(1);
        emitterRef.get().onNext(2);

        // Assert
        assertThat(received).isEmpty();
    }

    @Test
    @DisplayName("should allow only one terminal event (onError)")
    void shouldAllowOnlyOneTerminalEventOnError() {
        // Arrange
        List<String> events = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> events.add("onNext:" + item))
                        .onErrorAction(t -> events.add("onError:" + t.getMessage()))
                        .onCompleteAction(() -> events.add("onComplete"))
                        .build());

        // Act
        emitterRef.get().onNext(1);
        emitterRef.get().onError(new RuntimeException("Error 1"));
        emitterRef.get().onError(new RuntimeException("Error 2"));
        emitterRef.get().onComplete();

        // Assert
        assertThat(events)
                .hasSize(2)
                .containsExactly("onNext:1", "onError:Error 1");
    }

    @Test
    @DisplayName("should allow only one terminal event (onComplete)")
    void shouldAllowOnlyOneTerminalEventOnComplete() {
        // Arrange
        List<String> events = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> events.add("onNext:" + item))
                        .onErrorAction(t -> events.add("onError:" + t.getMessage()))
                        .onCompleteAction(() -> events.add("onComplete"))
                        .build());

        // Act
        emitterRef.get().onNext(1);
        emitterRef.get().onComplete();
        emitterRef.get().onNext(2);
        emitterRef.get().onError(new RuntimeException("Test error"));

        // Assert
        assertThat(events)
                .hasSize(2)
                .containsExactly("onNext:1", "onComplete");
    }

    @Test
    @DisplayName("should mark as disposed after onError")
    void shouldMarkAsDisposedAfterOnError() {
        // Arrange
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .build());

        // Act & Assert
        assertThat(emitterRef.get().isDisposed()).isFalse();

        emitterRef.get().onError(new RuntimeException("Test error"));

        assertThat(emitterRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("should mark as terminated but not disposed after onComplete")
    void shouldMarkAsTerminatedButNotDisposedAfterOnComplete() {
        // Arrange
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
        List<Integer> received = new ArrayList<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        // Act - complete, then try to emit more
        emitterRef.get().onComplete();
        emitterRef.get().onNext(99);

        // Assert - isDisposed is false, but no more items emitted due to terminated flag
        assertThat(emitterRef.get().isDisposed()).isFalse();
        assertThat(received).isEmpty();
    }

    @Test
    @DisplayName("should handle concurrent disposal from multiple threads during emission")
    void shouldHandleConcurrentDisposalFromMultipleThreadsDuringEmission() throws InterruptedException {
        // Arrange
        int iterations = 50;
        int disposeThreads = 5;

        for (int i = 0; i < iterations; i++) {
            List<Integer> received = new ArrayList<>(); // only emitter thread modifies the list
            AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
            CountDownLatch disposeStartLatch = new CountDownLatch(1);
            CountDownLatch disposeDoneLatch = new CountDownLatch(disposeThreads);

            Observable.create(emitterRef::set)
                    .subscribe(TestObserver.<Integer>builder()
                            .onNextAction(received::add)
                            .build());

            // Create multiple threads that will call dispose() concurrently
            List<Thread> disposeThreadsList = new ArrayList<>();
            for (int t = 0; t < disposeThreads; t++) {
                Thread disposeThread = new Thread(() -> {
                    try {
                        disposeStartLatch.await();
                        emitterRef.get().dispose();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        disposeDoneLatch.countDown();
                    }
                }, "dispose-thread-" + (t + 1));
                disposeThreadsList.add(disposeThread);
            }

            // Emitter thread - pauses at midpoint to let dispose threads get ready, then fires them all
            Thread emitterThread = buildEmitterThread(emitterRef, disposeStartLatch);
            disposeThreadsList.forEach(Thread::start);

            // Wait for completion
            assertThat(disposeDoneLatch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatCode(() -> emitterThread.join(5000)).doesNotThrowAnyException();

            assertThat(received.size()).isEqualTo(500); // 100 is a buffer for items in-flight before dispose visible
        }
    }

    private static Thread buildEmitterThread(AtomicReference<ObservableEmitter<Integer>> emitterRef, CountDownLatch disposeStartLatch) {
        Thread emitterThread = new Thread(() -> {
            for (int j = 0; j < 1000; j++) {
                emitterRef.get().onNext(j);
                // At midpoint, trigger all dispose threads simultaneously
                if (j == 499) {
                    disposeStartLatch.countDown();
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        fail(e);
                    }
                }
            }
            emitterRef.get().onComplete();
        }, "emitter-thread");

        // Start all threads
        emitterThread.start();
        return emitterThread;
    }
}
