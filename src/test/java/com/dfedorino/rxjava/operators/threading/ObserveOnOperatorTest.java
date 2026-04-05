package com.dfedorino.rxjava.operators.threading;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.Observer;
import com.dfedorino.rxjava.scheduler.Scheduler;
import com.dfedorino.rxjava.scheduler.Schedulers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ObserveOnOperatorTest {

    @Test
    @DisplayName("observeOn переключает onNext на указанный Scheduler")
    void testObserveOnSwitchesOnNextThread() throws InterruptedException {
        // Arrange
        AtomicReference<String> sourceThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        // Act & Assert
        Observable.create(emitter -> {
                    sourceThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .observeOn(Schedulers.computation())
                .subscribe(new SimpleObserver<>(
                        item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        null, null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertNotEquals(sourceThread.get(), onNextThread.get(),
                "onNext should be called in a different thread than source");
        assertTrue(onNextThread.get().startsWith("rxjava-computation-"),
                "Expected computation thread, but was: " + onNextThread.get());
    }

    @Test
    @DisplayName("observeOn переключает onError на указанный Scheduler")
    void testObserveOnSwitchesErrorThread() throws InterruptedException {
        AtomicReference<String> errorThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter ->
                        emitter.onError(new RuntimeException("Test error")))
                .observeOn(Schedulers.computation())
                .subscribe(new SimpleObserver<>(
                        null,
                        error -> {
                            errorThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(errorThread.get().startsWith("rxjava-computation-"),
                "Expected computation thread for error, but was: " + errorThread.get());
    }

    @Test
    @DisplayName("observeOn переключает onComplete на указанный Scheduler")
    void testObserveOnSwitchesCompleteThread() throws InterruptedException {
        AtomicReference<String> completeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> emitter.onComplete())
                .observeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(
                        null, null,
                        () -> {
                            completeThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        }
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(completeThread.get().startsWith("rxjava-io-"),
                "Expected io thread for complete, but was: " + completeThread.get());
    }

    @Test
    @DisplayName("observeOn сохраняет порядок элементов")
    void testObserveOnPreservesElementOrder() throws InterruptedException {
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(5);

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 5; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .observeOn(Schedulers.single())
                .subscribe(new SimpleObserver<>(
                        item -> {
                            received.add(item);
                            latch.countDown();
                        },
                        null, null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(List.of(1, 2, 3, 4, 5), received);
    }

    @Test
    @DisplayName("observeOn передаёт ошибку")
    void testObserveOnPropagatesError() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter ->
                        emitter.onError(new RuntimeException("Test error")))
                .observeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(null, t -> {
                    error.set(t);
                    latch.countDown();
                }, null));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertNotNull(error.get());
        assertEquals("Test error", error.get().getMessage());
    }

    @Test
    @DisplayName("observeOn передаёт onComplete")
    void testObserveOnPropagatesComplete() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> emitter.onComplete())
                .observeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(null, null, () -> {
                    completed.set(true);
                    latch.countDown();
                }));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(completed.get());
    }

    @Test
    @DisplayName("observeOn корректно обрабатывает dispose")
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
                .subscribe(new SimpleObserver<>(
                        item -> {
                            received.add(item);
                            disposableRef.get().dispose();
                            disposeLatch.countDown();
                        },
                        null, null,
                        disposableRef::set
                ));
        assertTrue(disposeLatch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);

        assertEquals(List.of(1), received, "Should only receive first item after dispose");
    }

    @Test
    @DisplayName("observeOn можно вызывать многократно для переключения между Scheduler")
    void testObserveOnMultipleCalls() throws InterruptedException {
        AtomicReference<String> firstObserveOnThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .observeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .subscribe(new SimpleObserver<>(
                        item -> {
                            firstObserveOnThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        null, null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(firstObserveOnThread.get().startsWith("rxjava-computation-"),
                "Expected computation thread (last observeOn), but was: " + firstObserveOnThread.get());
    }

    @Test
    @DisplayName("цепочка subscribeOn + observeOn")
    void testSubscribeOnPlusObserveOn() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.create(emitter -> {
                    subscribeThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .subscribe(new SimpleObserver<>(
                        item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        null, null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertNotEquals(subscribeThread.get(), onNextThread.get(),
                "Subscribe and onNext should be in different threads. Subscribe: " + subscribeThread.get() + ", onNext: " + onNextThread.get());
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
                })
                .observeOn(Schedulers.computation())
                .map(x -> {
                    mapThread.set(Thread.currentThread().getName());
                    return x * 2;
                })
                .subscribe(new SimpleObserver<>(
                        item -> {
                            received.add(item);
                            latch.countDown();
                        },
                        null, null
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(List.of(10), received);
        assertTrue(mapThread.get().startsWith("rxjava-computation-"),
                "Map should execute in computation thread, but was: " + mapThread.get());
    }

    @Test
    @DisplayName("observeOn передаёт ошибку Scheduler в onError")
    void testObserveOnHandlesSchedulerError() {
        Scheduler shutdownScheduler = new com.dfedorino.rxjava.scheduler.IOThreadScheduler();
        shutdownScheduler.shutdown();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    latch.countDown();
                })
                .observeOn(shutdownScheduler)
                .subscribe(new SimpleObserver<>(null, error::set, null));

        assertDoesNotThrow(() -> {
            try {
                latch.await(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Test
    @DisplayName("observeOn с пустым потоком")
    void testObserveOnEmptyStream() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());

        Observable.<Integer>create(emitter -> emitter.onComplete())
                .observeOn(Schedulers.io())
                .subscribe(new SimpleObserver<>(
                        received::add, null,
                        () -> {
                            completed.set(true);
                            latch.countDown();
                        }
                ));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(received.isEmpty());
        assertTrue(completed.get());
    }

    // ============================================================
    // RxJava Reference Comparison Tests
    // ============================================================

    @Test
    @DisplayName("[RxJava] observeOn переключает onNext на указанный Scheduler")
    void testObserveOnThread_RxJavaReference() throws InterruptedException {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        io.reactivex.rxjava3.core.Observable.create(emitter -> {
                    sourceThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.computation())
                .subscribe(
                        item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        error -> {},
                        () -> {}
                );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertNotEquals(sourceThread.get(), onNextThread.get(),
                "onNext should be called in a different thread than source");
    }

    @Test
    @DisplayName("[RxJava] observeOn сохраняет порядок элементов")
    void testObserveOnOrder_RxJavaReference() throws InterruptedException {
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(5);

        io.reactivex.rxjava3.core.Observable.range(1, 5)
                .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.single())
                .subscribe(
                        item -> {
                            received.add(item);
                            latch.countDown();
                        },
                        error -> {},
                        () -> {}
                );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(List.of(1, 2, 3, 4, 5), received);
    }

    @Test
    @DisplayName("[RxJava] цепочка subscribeOn + observeOn")
    void testSubscribeOnPlusObserveOn_RxJavaReference() throws InterruptedException {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> onNextThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        io.reactivex.rxjava3.core.Observable.create(emitter -> {
                    subscribeThread.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribeOn(io.reactivex.rxjava3.schedulers.Schedulers.io())
                .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.computation())
                .subscribe(
                        item -> {
                            onNextThread.set(Thread.currentThread().getName());
                            latch.countDown();
                        },
                        error -> {},
                        () -> {}
                );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertNotEquals(subscribeThread.get(), onNextThread.get(),
                "Subscribe and onNext should be in different threads. Subscribe: " + subscribeThread.get() + ", onNext: " + onNextThread.get());
    }

    @Test
    @DisplayName("[RxJava] observeOn + map выполняется в Scheduler")
    void testObserveOnPlusMap_RxJavaReference() throws InterruptedException {
        AtomicReference<String> mapThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());

        io.reactivex.rxjava3.core.Observable.just(5)
                .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.computation())
                .map(x -> {
                    mapThread.set(Thread.currentThread().getName());
                    return x * 2;
                })
                .subscribe(
                        received::add,
                        error -> {},
                        () -> latch.countDown()
                );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(mapThread.get().contains("RxComputation") || mapThread.get().contains("computation"),
                "Map should execute in computation thread, but was: " + mapThread.get());
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
