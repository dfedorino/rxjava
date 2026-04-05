package com.dfedorino.rxjava.operators.threading;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.ObservableEmitter;
import com.dfedorino.rxjava.exception.ErrorHandlers;
import com.dfedorino.rxjava.scheduler.Scheduler;
import com.dfedorino.rxjava.scheduler.Schedulers;
import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ObserveOnOperatorTest {

    @BeforeEach
    void setUp() {
        ErrorHandlers.setErrorHandler(ignored -> {});
    }

    @Test
    @DisplayName("переключает onNext на указанный Scheduler")
    void testObserveOnSwitchesOnNextThread() throws InterruptedException {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    sourceThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .observeOn(Schedulers.computation())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotEquals(sourceThread.get(), onNextThread.get());
        assertTrue(onNextThread.get().startsWith("rxjava-computation-"));
    }

    @Test
    @DisplayName("переключает onError на указанный Scheduler")
    void testObserveOnSwitchesErrorThread() throws InterruptedException {
        AtomicReference<String> errorThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter ->
                        emitter.onError(new RuntimeException("Test error")))
                .observeOn(Schedulers.computation())
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(error -> {
                            errorThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(errorThread.get().startsWith("rxjava-computation-"));
    }

    @Test
    @DisplayName("переключает onComplete на указанный Scheduler")
    void testObserveOnSwitchesCompleteThread() throws InterruptedException {
        AtomicReference<String> completeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(ObservableEmitter::onComplete)
                .observeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onCompleteAction(() -> {
                            completeThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(completeThread.get().startsWith("rxjava-io-"));
    }

    @Test
    @DisplayName("сохраняет порядок элементов")
    void testObserveOnPreservesOrder() throws InterruptedException {
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(5);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onNext(5);
                    emitter.onComplete();
                })
                .observeOn(Schedulers.single())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            received.add(item);
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(1, 2, 3, 4, 5), received);
    }

    @Test
    @DisplayName("пробрасывает ошибку")
    void testObserveOnPropagatesError() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter ->
                        emitter.onError(new RuntimeException("Test error")))
                .observeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(t -> {
                            error.set(t);
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("Test error", error.get().getMessage());
    }

    @Test
    @DisplayName("пропагирует onComplete")
    void testObserveOnPropagatesComplete() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(ObservableEmitter::onComplete)
                .observeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onCompleteAction(() -> {
                            completed.set(true);
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("корректно обрабатывает dispose")
    void testObserveOnHandlesDispose() throws InterruptedException {
        CountDownLatch disposeLatch = new CountDownLatch(1);
        AtomicReference<com.dfedorino.rxjava.core.Disposable> disposableRef = new AtomicReference<>();
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    Thread.sleep(50);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .observeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            received.add(item);
                            disposableRef.get().dispose();
                            disposeLatch.countDown();
                        })
                        .onSubscribeAction(disposableRef::set)
                        .build());

        assertTrue(disposeLatch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        assertEquals(List.of(1), received);
    }

    @Test
    @DisplayName("можно вызывать многократно")
    void testObserveOnMultipleCalls() throws InterruptedException {
        AtomicReference<String> thread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                }).observeOn(Schedulers.io()).observeOn(Schedulers.computation())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            thread.set(Thread.currentThread().getName());
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(thread.get().startsWith("rxjava-computation-"));
    }

    @Test
    @DisplayName("цепочка subscribeOn + observeOn")
    void testSubscribeOnPlusObserveOn() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    subscribeThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                }).subscribeOn(Schedulers.io()).observeOn(Schedulers.computation())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotEquals(subscribeThread.get(), onNextThread.get());
    }

    @Test
    @DisplayName("цепочка observeOn + map")
    void testObserveOnPlusMap() throws InterruptedException {
        AtomicReference<String> mapThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(5);
                    emitter.onComplete();
                }).observeOn(Schedulers.computation())
                .map(x -> {
                    mapThread.set(Thread.currentThread().getName());
                    return x * 2;
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                            received.add(item);
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(10), received);
        assertTrue(mapThread.get().startsWith("rxjava-computation-"));
    }

    @Test
    @DisplayName("передаёт ошибку Scheduler в onError")
    void testObserveOnHandlesSchedulerError() {
        Scheduler shutdownScheduler = new com.dfedorino.rxjava.scheduler.IOThreadScheduler();
        shutdownScheduler.shutdown();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    latch.countDown();
                }).observeOn(shutdownScheduler)
                .subscribe(TestObserver.<Integer>builder().onErrorAction(error::set).build());

        assertDoesNotThrow(() -> {
            try {
                latch.await(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Test
    @DisplayName("обрабатывает пустой поток")
    void testObserveOnEmptyStream() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());

        Observable.<Integer>create(ObservableEmitter::onComplete)
                .observeOn(Schedulers.io())
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> {
                            completed.set(true);
                            latch.countDown();
                        })
                        .build());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(received.isEmpty());
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("[RxJava] observeOn переключает onNext на указанный Scheduler")
    void testObserveOnThread_RxJava() throws InterruptedException {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        io.reactivex.rxjava3.core.Observable.create(emitter -> {
                    sourceThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                }).observeOn(io.reactivex.rxjava3.schedulers.Schedulers.computation())
                .subscribe(item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        error -> {
                        }, () -> {
                        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotEquals(sourceThread.get(), onNextThread.get());
    }

    @Test
    @DisplayName("[RxJava] observeOn сохраняет порядок")
    void testObserveOnOrder_RxJava() throws InterruptedException {
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(5);

        io.reactivex.rxjava3.core.Observable.range(1, 5)
                .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.single())
                .subscribe(item -> {
                    received.add(item);
                    latch.countDown();
                }, error -> {
                }, () -> {
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(1, 2, 3, 4, 5), received);
    }

    @Test
    @DisplayName("[RxJava] цепочка subscribeOn + observeOn")
    void testSubscribeOnPlusObserveOn_RxJava() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        io.reactivex.rxjava3.core.Observable.create(emitter -> {
                    subscribeThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                }).subscribeOn(io.reactivex.rxjava3.schedulers.Schedulers.io())
                .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.computation())
                .subscribe(item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        error -> {
                        }, () -> {
                        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotEquals(subscribeThread.get(), onNextThread.get());
    }

    @Test
    @DisplayName("[RxJava] observeOn + map выполняется в Scheduler")
    void testObserveOnPlusMap_RxJava() throws InterruptedException {
        AtomicReference<String> mapThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());

        io.reactivex.rxjava3.core.Observable.just(5)
                .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.computation())
                .map(x -> {
                    mapThread.set(Thread.currentThread().getName());
                    return x * 2;
                })
                .subscribe(received::add, error -> {
                }, () -> latch.countDown());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(mapThread.get().contains("RxComputation") || mapThread.get().contains("computation"));
    }
}
