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
    @DisplayName("полный цикл: create -> subscribeOn -> flatMap -> observeOn -> map -> filter -> subscribe")
    void shouldCompleteFullPipeline() throws InterruptedException {
        List<String> received = new ArrayList<>();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);

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

        assertThat(latch.await(5, TimeUnit.SECONDS)).as("Flow should complete").isTrue();
        assertThat(disposableRef.get()).isNotNull();
        assertThat(disposableRef.get().isDisposed()).isTrue();
        assertThat(received).containsExactly("Value-1", "Value-10", "Value-3", "Value-30", "Value-4", "Value-40");
        assertThat(completed.get()).isTrue();
    }

    @Test
    @DisplayName("dispose останавливает эмиссию")
    void shouldStopEmissionAfterDispose() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            emitterRef.set(emitter);
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.dispose();
            emitter.onNext(3);
            emitter.onNext(4);
            emitter.onComplete();
        }).subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).containsExactly(1, 2);
        assertThat(emitterRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("onError автоматически вызывает dispose")
    void shouldAutoDisposeOnError() {
        List<String> received = new ArrayList<>();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onError(testError);
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(TestObserver.<Integer>builder()
                .onSubscribeAction(disposableRef::set)
                .onNextAction(item -> received.add("onNext:" + item))
                .onErrorAction(t -> {
                    capturedError.set(t);
                    received.add("onError:" + t.getMessage());
                })
                .build());

        assertThat(received).containsExactly("onNext:1", "onError:Test error");
        assertThat(capturedError.get()).isSameAs(testError);
        assertThat(disposableRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("dispose() идемпотентен")
    void shouldHandleMultipleDisposeCalls() {
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> emitter.onNext(1))
                .subscribe(TestObserver.<Integer>builder().onSubscribeAction(disposableRef::set).build());

        disposableRef.get().dispose();
        disposableRef.get().dispose();
        disposableRef.get().dispose();

        assertThat(disposableRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("onNext блокируется после dispose")
    void shouldBlockOnNextAfterDispose() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            emitterRef.set(emitter);
            emitter.onNext(1);
            emitter.dispose();
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).containsExactly(1);
        assertThat(emitterRef.get().isDisposed()).isTrue();
    }

    @Test
    @DisplayName("ошибка пробрасывается через всю цепочку операторов")
    void shouldPropagateErrorThroughFullChain() throws InterruptedException {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        RuntimeException testError = new RuntimeException("Chain error");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .subscribeOn(Schedulers.computation())
                .map(i -> i * 2)
                .filter(i -> i > 0)
                .flatMap(x -> Observable.<Integer>create(e -> {
                    e.onNext(x);
                    e.onComplete();
                }))
                .observeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {})
                        .onErrorAction(t -> {
                            capturedError.set(t);
                            latch.countDown(); })
                        .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedError.get()).isSameAs(testError);
    }
}
