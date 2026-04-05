package com.dfedorino.rxjava.operators.threading;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.scheduler.IOThreadScheduler;
import com.dfedorino.rxjava.scheduler.Scheduler;
import com.dfedorino.rxjava.scheduler.Schedulers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SubscribeOnOperatorTest {

    @Test
    @DisplayName("subscribeOn выполняет подписку в указанном потоке")
    void testSubscribeOnRunsInSchedulerThread() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    subscribeThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                    latch.countDown();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(received::add, null, null));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(received.contains(1));
        assertTrue(subscribeThread.get().startsWith("rxjava-io-"),
                "Expected thread starting with 'rxjava-io-', but was: " + subscribeThread.get());
    }

    @Test
    @DisplayName("subscribeOn передаёт onNext в том же потоке, где источник генерирует элементы")
    void testSubscribeOnDoesNotSwitchOnNextThread() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    subscribeThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                    latch.countDown();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(
                        item -> onNextThread.set(Thread.currentThread().getName()),
                        null, null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(subscribeThread.get(), onNextThread.get(),
                "onNext should be called in the same thread as subscription");
    }

    @Test
    @DisplayName("subscribeOn передаёт onError")
    void testSubscribeOnPropagatesError() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onError(new RuntimeException("Test error"));
                    latch.countDown();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(null, error::set, null));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertNotNull(error.get());
        assertEquals("Test error", error.get().getMessage());
    }

    @Test
    @DisplayName("subscribeOn передаёт onComplete")
    void testSubscribeOnPropagatesComplete() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> {
                    emitter.onComplete();
                    latch.countDown();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(null, null, () -> {
                    completed.set(true);
                    latch.countDown();
                }));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(completed.get());
    }

    @Test
    @DisplayName("subscribeOn обрабатывает dispose до начала генерации элементов")
    void testSubscribeOnHandlesDisposeBeforeEmission() throws InterruptedException {
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(1);
        AtomicBoolean disposed = new AtomicBoolean(false);
        AtomicReference<com.dfedorino.rxjava.core.Disposable> disposableRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    subscribeLatch.countDown();
                    Thread.sleep(500);
                    disposed.set(emitter.isDisposed());
                    if (!emitter.isDisposed()) {
                        emitter.onNext(1);
                    }
                    emitter.onComplete();
                    doneLatch.countDown();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(
                        item -> {}, null, null,
                        disposableRef::set
                ));

        assertTrue(subscribeLatch.await(5, TimeUnit.SECONDS));
        disposableRef.get().dispose();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));

        assertTrue(disposed.get(), "Source should have been disposed before emission");
    }

    @Test
    @DisplayName("subscribeOn передаёт ошибку Scheduler в onError")
    void testSubscribeOnHandlesSchedulerError() {
        Scheduler shutdownScheduler = new IOThreadScheduler();
        shutdownScheduler.shutdown();

        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter -> fail("Should not subscribe"))
                .subscribeOn(shutdownScheduler)
                .subscribe(new SimpleObserver<>(null, error::set, null));

        assertNotNull(error.get());
    }

    @Test
    @DisplayName("subscribeOn можно вызывать многократно")
    void testSubscribeOnMultipleCalls() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> firstThread = new AtomicReference<>();
        AtomicReference<String> secondThread = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    firstThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                    latch.countDown();
                })
                .subscribeOn(Schedulers.io())
                .subscribeOn(Schedulers.computation())
                .subscribe(new SimpleObserver<>(
                        item -> secondThread.set(Thread.currentThread().getName()),
                        null, null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(firstThread.get().startsWith("rxjava-io-"),
                "Expected subscribeOn thread, but was: " + firstThread.get());
        assertEquals(firstThread.get(), secondThread.get(),
                "onNext should be in same thread as subscription");
    }

    // Helper observer
    private static class SimpleObserver<T> implements Observer<T> {
        private final java.util.function.Consumer<T> onNextAction;
        private final java.util.function.Consumer<Throwable> onErrorAction;
        private final Runnable onCompleteAction;
        private final java.util.function.Consumer<com.dfedorino.rxjava.core.Disposable> onSubscribeAction;

        SimpleObserver(java.util.function.Consumer<T> onNextAction,
                       java.util.function.Consumer<Throwable> onErrorAction,
                       Runnable onCompleteAction) {
            this(onNextAction, onErrorAction, onCompleteAction, null);
        }

        SimpleObserver(java.util.function.Consumer<T> onNextAction,
                       java.util.function.Consumer<Throwable> onErrorAction,
                       Runnable onCompleteAction,
                       java.util.function.Consumer<com.dfedorino.rxjava.core.Disposable> onSubscribeAction) {
            this.onNextAction = onNextAction;
            this.onErrorAction = onErrorAction != null ? onErrorAction : t -> {};
            this.onCompleteAction = onCompleteAction != null ? onCompleteAction : () -> {};
            this.onSubscribeAction = onSubscribeAction != null ? onSubscribeAction : d -> {};
        }

        @Override
        public void onSubscribe(com.dfedorino.rxjava.core.Disposable d) {
            onSubscribeAction.accept(d);
        }

        @Override
        public void onNext(T item) {
            if (onNextAction != null) {
                try {
                    onNextAction.accept(item);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            onErrorAction.accept(t);
        }

        @Override
        public void onComplete() {
            onCompleteAction.run();
        }
    }
}
