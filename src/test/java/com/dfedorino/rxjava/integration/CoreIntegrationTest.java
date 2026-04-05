package com.dfedorino.rxjava.integration;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.ObservableEmitter;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.scheduler.Schedulers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CoreIntegrationTest {

    @Test
    @DisplayName("Базовый поток данных: Observable создаётся через create(), Observer получает элементы и завершает поток")
    void testCompleteFlow() throws InterruptedException {
        List<String> received = new ArrayList<>();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onComplete();
                })
                .subscribeOn(Schedulers.computation())
                .flatMap(x -> {
                    System.out.println("[" + Thread.currentThread().getName() + "] called flatMap");
                    return Observable.<Integer>create(e -> {
                        e.onNext(x);
                        e.onNext(x * 10);
                        e.onComplete();
                    });
                })
                .subscribeOn(Schedulers.io())
                .map(i -> "Value-" + i)
                .filter(s -> !s.contains("Value-2"))
                .subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                        disposableRef.set(d);
                    }

                    @Override
                    public void onNext(String item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("onError should not be called");
                    }

                    @Override
                    public void onComplete() {
                        completed.set(true);
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Flow did not complete within timeout");

        assertNotNull(disposableRef.get());
        assertTrue(disposableRef.get().isDisposed());
        assertEquals(List.of("Value-1", "Value-10", "Value-3", "Value-30", "Value-4", "Value-40"), received);
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("Отмена подписки через dispose() останавливает эмиссию элементов")
    void testDisposeStopsEmission() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitterRef.set(emitter);
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onComplete();
                })
                .subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                    }

                    @Override
                    public void onNext(Integer item) {
                        received.add(item);
                        if (item == 2) {
                            emitterRef.get().dispose();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("onError should not be called");
                    }

                    @Override
                    public void onComplete() {
                        fail("onComplete should not be called");
                    }
                });

        assertEquals(List.of(1, 2), received);
        assertTrue(emitterRef.get().isDisposed());
    }

    @Test
    @DisplayName("onError() автоматически завершает подписку (isDisposed = true)")
    void testErrorDisposesAutomatically() {
        List<String> received = new ArrayList<>();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        Throwable[] capturedError = new Throwable[1];
        RuntimeException testError = new RuntimeException("Test error");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                    emitter.onNext(2);
                })
                .subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                        disposableRef.set(d);
                    }

                    @Override
                    public void onNext(Integer item) {
                        received.add("onNext:" + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        capturedError[0] = t;
                        received.add("onError:" + t.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        fail("onComplete should not be called");
                    }
                });

        assertEquals(2, received.size());
        assertEquals("onNext:1", received.get(0));
        assertEquals("onError:Test error", received.get(1));
        assertEquals(testError, capturedError[0]);
        assertTrue(disposableRef.get().isDisposed());

        disposableRef.get().dispose();
        assertTrue(disposableRef.get().isDisposed());
    }

    @Test
    @DisplayName("Идемпотентность dispose(): многократный вызов не вызывает ошибок")
    void testDisposeIdempotency() {
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                        disposableRef.set(d);
                    }

                    @Override
                    public void onNext(Integer item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("onError should not be called");
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        disposableRef.get().dispose();
        disposableRef.get().dispose();
        disposableRef.get().dispose();

        assertTrue(disposableRef.get().isDisposed());
    }

    @Test
    @DisplayName("onNext() не проходит после dispose() благодаря проверке !get()")
    void testOnNextBlockedAfterDispose() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitterRef.set(emitter);
                    emitter.onNext(1);
                    emitter.dispose();
                    emitter.onNext(2);
                    emitter.onNext(3);
                })
                .subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                    }

                    @Override
                    public void onNext(Integer item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("onError should not be called");
                    }

                    @Override
                    public void onComplete() {
                        fail("onComplete should not be called");
                    }
                });

        assertEquals(List.of(1), received);
        assertTrue(emitterRef.get().isDisposed());
    }
}
