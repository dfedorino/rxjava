package com.dfedorino.rxjava.operators.threading;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.scheduler.IOThreadScheduler;
import com.dfedorino.rxjava.scheduler.Scheduler;
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

import static org.junit.jupiter.api.Assertions.*;

class SubscribeOnOperatorTest {

    @Test
    @DisplayName("выполняет подписку в указанном потоке")
    void testSubscribeOnRunsInSchedulerThread() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            subscribeThread.set(Thread.currentThread().getName());
            emitter.onNext(1); emitter.onComplete(); latch.countDown();
        }).subscribeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(received.contains(1));
        assertTrue(subscribeThread.get().startsWith("rxjava-io-"));
    }

    @Test
    @DisplayName("не переключает поток для onNext")
    void testSubscribeOnDoesNotSwitchOnNextThread() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            subscribeThread.set(Thread.currentThread().getName());
            emitter.onNext(1); emitter.onComplete(); latch.countDown();
        }).subscribeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> onNextThread.set(Thread.currentThread().getName()))
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(subscribeThread.get(), onNextThread.get());
    }

    @Test
    @DisplayName("пробрасывает ошибку")
    void testSubscribeOnPropagatesError() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            emitter.onError(new RuntimeException("Test error")); latch.countDown();
        }).subscribeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder().onErrorAction(error::set).build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("Test error", error.get().getMessage());
    }

    @Test
    @DisplayName("пропагирует onComplete")
    void testSubscribeOnPropagatesComplete() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
            emitter.onComplete(); latch.countDown();
        }).subscribeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onCompleteAction(() -> { completed.set(true); latch.countDown(); })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("обрабатывает dispose до начала emisiи")
    void testSubscribeOnHandlesDisposeBeforeEmission() throws InterruptedException {
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(1);
        AtomicBoolean disposed = new AtomicBoolean();
        AtomicReference<com.dfedorino.rxjava.core.Disposable> disposableRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            subscribeLatch.countDown();
            Thread.sleep(500);
            disposed.set(emitter.isDisposed());
            if (!emitter.isDisposed()) emitter.onNext(1);
            emitter.onComplete(); doneLatch.countDown();
        }).subscribeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onSubscribeAction(disposableRef::set)
                        .build());

        assertTrue(subscribeLatch.await(5, TimeUnit.SECONDS));
        disposableRef.get().dispose();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));
        assertTrue(disposed.get());
    }

    @Test
    @DisplayName("передаёт ошибку Scheduler в onError")
    void testSubscribeOnHandlesSchedulerError() {
        Scheduler shutdownScheduler = new IOThreadScheduler();
        shutdownScheduler.shutdown();
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter -> fail("Should not subscribe"))
                .subscribeOn(shutdownScheduler)
                .subscribe(TestObserver.<Integer>builder().onErrorAction(error::set).build());

        assertNotNull(error.get());
    }

    @Test
    @DisplayName("можно вызывать многократно")
    void testSubscribeOnMultipleCalls() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> firstThread = new AtomicReference<>();
        AtomicReference<String> secondThread = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            firstThread.set(Thread.currentThread().getName());
            emitter.onNext(1); emitter.onComplete(); latch.countDown();
        }).subscribeOn(Schedulers.io()).subscribeOn(Schedulers.computation())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> secondThread.set(Thread.currentThread().getName()))
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(firstThread.get().startsWith("rxjava-io-"));
        assertEquals(firstThread.get(), secondThread.get());
    }
}
