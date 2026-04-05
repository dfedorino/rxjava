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

class CreateEmitterTest {

    @Test
    @DisplayName("останавливает эмиссию элементов после dispose")
    void shouldStopAfterDispose() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        emitterRef.get().dispose();
        emitterRef.get().onNext(1);
        emitterRef.get().onNext(2);

        assertThat(received).isEmpty();
    }

    @Test
    @DisplayName("допускает только одно терминальное событие (onError)")
    void shouldAllowOnlyOneTerminalOnError() {
        List<String> events = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> events.add("onNext:" + item))
                        .onErrorAction(t -> events.add("onError:" + t.getMessage()))
                        .onCompleteAction(() -> events.add("onComplete"))
                        .build());

        emitterRef.get().onNext(1);
        emitterRef.get().onError(new RuntimeException("Error 1"));
        emitterRef.get().onError(new RuntimeException("Error 2"));
        emitterRef.get().onComplete();

        assertThat(events)
                .hasSize(2)
                .containsExactly("onNext:1", "onError:Error 1");
    }

    @Test
    @DisplayName("допускает только одно терминальное событие (onComplete)")
    void shouldAllowOnlyOneTerminalOnComplete() {
        List<String> events = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> events.add("onNext:" + item))
                        .onCompleteAction(() -> events.add("onComplete"))
                        .build());

        emitterRef.get().onNext(1);
        emitterRef.get().onComplete();
        emitterRef.get().onNext(2);
        emitterRef.get().onError(new RuntimeException("Test error"));

        assertThat(events)
                .hasSize(2)
                .containsExactly("onNext:1", "onComplete");
    }

    @Test
    @DisplayName("помечает как disposed после onError")
    void shouldMarkDisposedAfterOnError() {
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder().build());

        assertThat(emitterRef.get().isDisposed()).isFalse();
        emitterRef.get().onError(new RuntimeException("Test error"));
        assertThat(emitterRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("помечает как terminated, но не disposed после onComplete")
    void shouldMarkTerminatedNotDisposedAfterOnComplete() {
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
        List<Integer> received = new ArrayList<>();

        Observable.create(emitterRef::set)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        emitterRef.get().onComplete();
        emitterRef.get().onNext(99);

        assertThat(emitterRef.get().isDisposed()).isFalse();
        assertThat(received).isEmpty();
    }

    @Test
    @DisplayName("потокобезопасность dispose")
    void shouldHandleConcurrentDisposal() throws InterruptedException {
        int iterations = 50, disposeThreads = 5;

        for (int i = 0; i < iterations; i++) {
            List<Integer> received = new ArrayList<>();
            AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(disposeThreads);

            Observable.create(emitterRef::set)
                    .subscribe(TestObserver.<Integer>builder()
                            .onNextAction(received::add)
                            .build());

            List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < disposeThreads; t++) {
                Thread thread = new Thread(() -> {
                    try {
                        startLatch.await();
                        emitterRef.get().dispose();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                }, "dispose-" + t);
                threads.add(thread);
            }

            Thread emitter = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    emitterRef.get().onNext(j);
                    if (j == 499) {
                        startLatch.countDown();
                        try {
                            TimeUnit.MILLISECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            fail(e);
                        }
                    }
                }
                emitterRef.get().onComplete();
            });

            threads.forEach(Thread::start);
            emitter.start();

            assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatCode(() -> emitter.join(5000)).doesNotThrowAnyException();
            assertThat(received.size()).isEqualTo(500);
        }
    }
}
