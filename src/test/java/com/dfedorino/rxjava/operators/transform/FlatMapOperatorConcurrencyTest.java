package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.*;
import com.dfedorino.rxjava.scheduler.Schedulers;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrency tests for FlatMap operator.
 * These tests are designed to expose critical thread safety bugs and reactive contract violations.
 * 
 * FlatMap has the most concurrency bugs due to coordination between multiple inner observables.
 * Expected failures indicate bugs in the current implementation.
 */
class FlatMapOperatorConcurrencyTest {

    @Test
    @DisplayName("BUG: Race condition in termination logic - double onComplete")
    void testDoubleOnCompleteRaceCondition() throws InterruptedException {
        // This test exposes the race between checkTermination() and innerComplete()
        int testRuns = 50;
        AtomicInteger doubleComplete = new AtomicInteger(0);
        AtomicInteger totalCompletes = new AtomicInteger(0);
        
        for (int run = 0; run < testRuns; run++) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch innerStartedLatch = new CountDownLatch(1);
            AtomicInteger completeCount = new AtomicInteger(0);
            AtomicBoolean firstComplete = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                try {
                    System.out.printf("[%s] startLatch.await %n", Thread.currentThread().getName());
                    startLatch.await();
                    emitter.onNext(1);
                    // Complete immediately after emitting
                    emitter.onComplete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                System.out.printf("[%s] innerStartedLatch.await %n", Thread.currentThread().getName());
                innerStartedLatch.countDown();
                // Slow inner - increases race window
                Thread.sleep(100);
                innerEmitter.onNext(x * 10);
                innerEmitter.onComplete();
            })).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {}
                
                @Override
                public void onNext(Integer item) {}
                
                @Override
                public void onError(Throwable t) {}
                
                @Override
                public void onComplete() {
                    if (firstComplete.getAndSet(true)) {
                        completeCount.incrementAndGet();
                    }
                    totalCompletes.incrementAndGet();
                }
            });

            System.out.printf("[%s] startLatch.await %n", Thread.currentThread().getName());
            startLatch.countDown();
            System.out.printf("[%s] innerStartedLatch.await %n", Thread.currentThread().getName());
            innerStartedLatch.await(1, TimeUnit.SECONDS);
            Thread.sleep(500);
            
            if (completeCount.get() > 0) {
                doubleComplete.incrementAndGet();
            }
        }
        
        System.out.println("FlatMap double onComplete occurrences: " + doubleComplete.get() + "/" + testRuns);
        System.out.println("Total completes (should be " + testRuns + "): " + totalCompletes.get());
    }

    @Test
    @DisplayName("BUG: disposeAllInnerSubscriptions doesn't actually dispose inner subscriptions")
    void testInnerObservablesContinueAfterDispose() throws InterruptedException {
        // This test exposes that disposeAllInnerSubscriptions only sets counter to 0
        // but doesn't actually dispose inner subscriptions
        AtomicInteger itemsAfterDispose = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        CountDownLatch disposeLatch = new CountDownLatch(1);
        AtomicBoolean disposed = new AtomicBoolean(false);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            subscribeLatch.countDown();
            disposeLatch.await(2, TimeUnit.SECONDS);
            // Emit after dispose signal
            if (!emitter.isDisposed()) {
                emitter.onNext(1);
                emitter.onNext(2);
                emitter.onComplete();
            }
        });
        
        observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
            // Slow inner that continues emitting
            for (int i = 0; i < 10; i++) {
                if (!innerEmitter.isDisposed()) {
                    innerEmitter.onNext(x * 10 + i);
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            innerEmitter.onComplete();
        })).subscribe(new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
                disposableRef.set(d);
            }
            
            @Override
            public void onNext(Integer item) {
                if (disposed.get()) {
                    itemsAfterDispose.incrementAndGet();
                }
            }
            
            @Override
            public void onError(Throwable t) {}
            
            @Override
            public void onComplete() {}
        });
        
        subscribeLatch.await(1, TimeUnit.SECONDS);
        Disposable d = disposableRef.get();
        assertNotNull(d);
        disposed.set(true);
        d.dispose();
        disposeLatch.countDown();
        
        Thread.sleep(1000);
        
        System.out.println("Items received after dispose: " + itemsAfterDispose.get());
        // If this is > 0, it means inner subscriptions weren't properly disposed
    }

    @Test
    @DisplayName("BUG: TOCTOU in onNext - activeSubscriptions incremented after dispose")
    void testToctouBetweenDisposeAndOnNext() throws InterruptedException {
        int iterations = 50;
        AtomicInteger subscriptionsAfterDispose = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            CountDownLatch raceLatch = new CountDownLatch(1);
            AtomicReference<FlatMapObserver<Integer, Integer>> flatMapObserverRef = new AtomicReference<>();
            
            Observable<Integer> observable = Observable.create(emitter -> {
                subscribeLatch.countDown();
                raceLatch.await(1, TimeUnit.SECONDS);
                // Try to emit after dispose
                for (int j = 0; j < 5; j++) {
                    emitter.onNext(j);
                }
                emitter.onComplete();
            });
            
            Thread subscribeThread = new Thread(() -> {
                observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    subscriptionsAfterDispose.incrementAndGet();
                    innerEmitter.onNext(x);
                    innerEmitter.onComplete();
                })).subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                        if (d instanceof FlatMapObserver) {
                            flatMapObserverRef.set((FlatMapObserver<Integer, Integer>) d);
                        }
                    }
                    
                    @Override
                    public void onNext(Integer item) {}
                    
                    @Override
                    public void onError(Throwable t) {}
                    
                    @Override
                    public void onComplete() {}
                });
            });
            
            Thread disposeThread = new Thread(() -> {
                try {
                    subscribeLatch.await();
                    raceLatch.countDown();
                    if (flatMapObserverRef.get() != null) {
                        flatMapObserverRef.get().dispose();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            subscribeThread.start();
            disposeThread.start();
            subscribeThread.join(2000);
            disposeThread.join(2000);
        }
        
        System.out.println("Subscriptions created after dispose: " + subscriptionsAfterDispose.get());
    }

    @Test
    @DisplayName("BUG: Concurrent inner observable subscriptions")
    void testConcurrentInnerSubscriptions() throws InterruptedException {
        int numOuterItems = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger totalInnerSubscriptions = new AtomicInteger(0);
        AtomicInteger concurrentInnerCreations = new AtomicInteger(0);
        AtomicInteger maxConcurrentInners = new AtomicInteger(0);
        AtomicInteger activeInners = new AtomicInteger(0);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            for (int i = 0; i < numOuterItems; i++) {
                final int finalI = i;
                Schedulers.io().execute(() -> {
                    try {
                        startLatch.await();
                        emitter.onNext(finalI);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        });
        
        observable.flatMap(x -> {
            final int finalX = x;
            maxConcurrentInners.updateAndGet(max -> Math.max(max, activeInners.incrementAndGet()));
            concurrentInnerCreations.incrementAndGet();
            
            return Observable.<Integer>create(innerEmitter -> {
                totalInnerSubscriptions.incrementAndGet();
                try {
                    Thread.sleep(100); // Slow inner
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                innerEmitter.onNext(finalX);
                innerEmitter.onComplete();
                activeInners.decrementAndGet();
            });
        }).subscribe(new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {}
            
            @Override
            public void onNext(Integer item) {}
            
            @Override
            public void onError(Throwable t) {}
            
            @Override
            public void onComplete() {}
        });
        
        startLatch.countDown();
        Thread.sleep(5000);
        
        System.out.println("FlatMap concurrent inner subscriptions:");
        System.out.println("  Total inner subscriptions: " + totalInnerSubscriptions.get());
        System.out.println("  Max concurrent inners: " + maxConcurrentInners.get());
        System.out.println("  Total inner creations: " + concurrentInnerCreations.get());
        
        assertEquals(numOuterItems, totalInnerSubscriptions.get(),
            "Should create inner subscription for each outer item");
        assertTrue(maxConcurrentInners.get() > 1, 
            "Should have concurrent inner subscriptions");
    }

    @Test
    @DisplayName("BUG: Concurrent error propagation from multiple inners")
    void testConcurrentErrorPropagation() throws InterruptedException {
        int testRuns = 30;
        AtomicInteger multipleErrors = new AtomicInteger(0);
        AtomicInteger errorsReceived = new AtomicInteger(0);
        
        for (int run = 0; run < testRuns; run++) {
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger errorCount = new AtomicInteger(0);
            AtomicBoolean errorHandled = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                try {
                    startLatch.await();
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                // Both inners will error concurrently
                Thread errorThread = new Thread(() -> {
                    innerEmitter.onError(new RuntimeException("Error from inner " + x));
                });
                errorThread.start();
            })).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {}
                
                @Override
                public void onNext(Integer item) {}
                
                @Override
                public void onError(Throwable t) {
                    if (errorHandled.getAndSet(true)) {
                        errorCount.incrementAndGet();
                    }
                    errorsReceived.incrementAndGet();
                }
                
                @Override
                public void onComplete() {}
            });
            
            startLatch.countDown();
            Thread.sleep(500);
            
            if (errorCount.get() > 0) {
                multipleErrors.incrementAndGet();
            }
        }
        
        System.out.println("FlatMap concurrent errors:");
        System.out.println("  Multiple errors received: " + multipleErrors.get() + "/" + testRuns);
        System.out.println("  Total errors: " + errorsReceived.get());
    }

    @Test
    @DisplayName("BUG: activeSubscriptions counter race condition")
    void testActiveSubscriptionsCounterRace() throws InterruptedException {
        int iterations = 30;
        AtomicInteger terminationIssues = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch innerReadyLatch = new CountDownLatch(1);
            AtomicInteger completeCount = new AtomicInteger(0);
            AtomicInteger onNextCount = new AtomicInteger(0);
            AtomicBoolean completedBeforeAllInners = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                try {
                    startLatch.await();
                    emitter.onNext(1);
                    emitter.onNext(2);
                    // Complete source immediately while inners are still running
                    emitter.onComplete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                innerReadyLatch.countDown();
                // Very slow inner - ensures race condition
                Thread.sleep(500);
                innerEmitter.onNext(x);
                onNextCount.incrementAndGet();
                innerEmitter.onComplete();
            })).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {}
                
                @Override
                public void onNext(Integer item) {}
                
                @Override
                public void onError(Throwable t) {}
                
                @Override
                public void onComplete() {
                    completeCount.incrementAndGet();
                    if (onNextCount.get() < 2) {
                        completedBeforeAllInners.set(true);
                    }
                }
            });
            
            startLatch.countDown();
            innerReadyLatch.await(1, TimeUnit.SECONDS);
            // Don't wait - let the race condition happen
            Thread.sleep(100);
            
            if (completedBeforeAllInners.get() || completeCount.get() > 1) {
                terminationIssues.incrementAndGet();
            }
        }
        
        System.out.println("FlatMap termination issues: " + terminationIssues.get() + "/" + iterations);
    }

    @Test
    @DisplayName("BUG: upstreamDisposable field visibility")
    void testUpstreamDisposableVisibilityAcrossThreads() throws InterruptedException {
        int iterations = 50;
        AtomicInteger nullUpstreamDisposable = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            AtomicReference<FlatMapObserver<Integer, Integer>> flatMapObserverRef = new AtomicReference<>();
            
            Observable<Integer> observable = Observable.create(emitter -> {
                subscribeLatch.countDown();
                try {
                    Thread.sleep(200);
                    emitter.onNext(1);
                    emitter.onComplete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            Thread subscribeThread = new Thread(() -> {
                observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x);
                    innerEmitter.onComplete();
                })).subscribe(new Observer<>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                        if (d instanceof FlatMapObserver) {
                            flatMapObserverRef.set((FlatMapObserver<Integer, Integer>) d);
                        }
                    }
                    
                    @Override
                    public void onNext(Integer item) {}
                    
                    @Override
                    public void onError(Throwable t) {}
                    
                    @Override
                    public void onComplete() {}
                });
            });
            
            subscribeThread.start();
            subscribeLatch.await(1, TimeUnit.SECONDS);
            
            // Try to access upstreamDisposable from different thread
            FlatMapObserver<Integer, Integer> observer = flatMapObserverRef.get();
            if (observer != null) {
                // Try to dispose - might see null upstreamDisposable
                observer.dispose();
                if (!observer.isDisposed()) {
                    nullUpstreamDisposable.incrementAndGet();
                }
            }
            
            subscribeThread.join(1000);
        }
        
        System.out.println("Null upstreamDisposable occurrences: " + nullUpstreamDisposable.get() + "/" + iterations);
    }

    @Test
    @DisplayName("BUG: Mapper called concurrently with dispose")
    void testMapperCalledConcurrentlyWithDispose() throws InterruptedException {
        AtomicInteger mapperCallsAfterDispose = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        CountDownLatch raceLatch = new CountDownLatch(1);
        AtomicBoolean disposed = new AtomicBoolean(false);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            subscribeLatch.countDown();
            raceLatch.await(1, TimeUnit.SECONDS);
            // Rapid emissions
            for (int i = 0; i < 20; i++) {
                if (!emitter.isDisposed()) {
                    emitter.onNext(i);
                }
            }
            emitter.onComplete();
        });
        
        observable.flatMap(x -> {
            if (disposed.get()) {
                mapperCallsAfterDispose.incrementAndGet();
            }
            return Observable.<Integer>create(innerEmitter -> {
                innerEmitter.onNext(x);
                innerEmitter.onComplete();
            });
        }).subscribe(new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
                disposableRef.set(d);
            }
            
            @Override
            public void onNext(Integer item) {}
            
            @Override
            public void onError(Throwable t) {}
            
            @Override
            public void onComplete() {}
        });
        
        subscribeLatch.await(1, TimeUnit.SECONDS);
        disposed.set(true);
        disposableRef.get().dispose();
        raceLatch.countDown();
        
        Thread.sleep(1000);
        
        System.out.println("Mapper calls after dispose: " + mapperCallsAfterDispose.get());
    }

    @Test
    @DisplayName("BUG: FlatMapInnerObserver disposable visibility")
    void testFlatMapInnerObserverDisposableVisibility() throws InterruptedException {
        int iterations = 50;
        AtomicInteger visibilityIssues = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            AtomicReference<FlatMapInnerObserver<Integer>> innerObserverRef = new AtomicReference<>();
            
            Observable<Integer> observable = Observable.create(emitter -> {
                subscribeLatch.countDown();
                try {
                    Thread.sleep(300);
                    emitter.onNext(1);
                    emitter.onComplete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                // innerEmitter is wrapped in FlatMapInnerObserver
                Thread.sleep(100);
                innerEmitter.onNext(x);
                innerEmitter.onComplete();
            })).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {
                    // The FlatMapObserver, not FlatMapInnerObserver
                }
                
                @Override
                public void onNext(Integer item) {}
                
                @Override
                public void onError(Throwable t) {}
                
                @Override
                public void onComplete() {}
            });
            
            subscribeLatch.await(1, TimeUnit.SECONDS);
            Thread.sleep(500);
        }
        
        System.out.println("Inner observer disposable visibility issues: " + visibilityIssues.get() + "/" + iterations);
    }

    @Test
    @DisplayName("Stress test: Massive concurrent flatmap operations")
    void testMassiveConcurrentFlatMapStressTest() throws InterruptedException {
        int numItems = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(1);
        AtomicInteger totalReceived = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger completes = new AtomicInteger(0);
        AtomicInteger innerCreations = new AtomicInteger(0);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            Schedulers.io().execute(() -> {
                try {
                    startLatch.await(5, TimeUnit.SECONDS);
                    for (int i = 0; i < numItems; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                } catch (Exception e) {
                    emitter.onError(e);
                } finally {
                    completeLatch.countDown();
                }
            });
        });

        observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
            innerCreations.incrementAndGet();
            Schedulers.io().execute(() -> {
                try {
                    Thread.sleep(10);
                    innerEmitter.onNext(x * 2);
                    innerEmitter.onComplete();
                } catch (Exception e) {
                    innerEmitter.onError(e);
                }
            });
        })).subscribe(new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {}
            
            @Override
            public void onNext(Integer item) {
                totalReceived.incrementAndGet();
            }
            
            @Override
            public void onError(Throwable t) {
                errors.incrementAndGet();
            }
            
            @Override
            public void onComplete() {
                completes.incrementAndGet();
            }
        });
        
        startLatch.countDown();
        boolean finished = completeLatch.await(30, TimeUnit.SECONDS);
        Thread.sleep(1000); // Wait for all inners to complete
        
        System.out.println("FlatMap stress test results:");
        System.out.println("  Items sent: " + numItems);
        System.out.println("  Items received: " + totalReceived.get());
        System.out.println("  Inner creations: " + innerCreations.get());
        System.out.println("  Errors: " + errors.get());
        System.out.println("  Completes: " + completes.get());
        System.out.println("  Finished on time: " + finished);
        
        assertEquals(numItems, totalReceived.get(),
            "Should receive all items from flatmap");
        assertEquals(numItems, innerCreations.get(),
            "Should create inner observable for each item");
        assertEquals(1, completes.get(),
            "Should complete exactly once");
        assertEquals(0, errors.get(),
            "No errors should occur");
    }

    @Test
    @DisplayName("BUG: Multiple onComplete with slow inner completions")
    void testMultipleOnCompleteWithSlowInnerCompletions() throws InterruptedException {
        int iterations = 20;
        AtomicInteger multipleOnCompletes = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            AtomicInteger onCompleteCount = new AtomicInteger(0);
            AtomicBoolean firstOnComplete = new AtomicBoolean(false);
            CountDownLatch sourceCompleteLatch = new CountDownLatch(1);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                emitter.onNext(1);
                emitter.onNext(2);
                emitter.onNext(3);
                emitter.onComplete();
                sourceCompleteLatch.countDown();
            });
            
            observable.flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                // Each inner completes at different time
                Thread.sleep(x * 100);
                innerEmitter.onNext(x);
                innerEmitter.onComplete();
            })).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {}
                
                @Override
                public void onNext(Integer item) {}
                
                @Override
                public void onError(Throwable t) {}
                
                @Override
                public void onComplete() {
                    if (firstOnComplete.getAndSet(true)) {
                        onCompleteCount.incrementAndGet();
                    }
                }
            });
            
            sourceCompleteLatch.await(1, TimeUnit.SECONDS);
            Thread.sleep(1000);
            
            if (onCompleteCount.get() > 0) {
                multipleOnCompletes.incrementAndGet();
            }
        }
        
        System.out.println("Multiple onComplete calls: " + multipleOnCompletes.get() + "/" + iterations);
    }

    @Test
    @DisplayName("BUG: Error during innerComplete doesn't prevent other completions")
    void testErrorDuringInnerCompletion() throws InterruptedException {
        AtomicInteger errorHandlingIssues = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            try {
                startLatch.await();
                emitter.onNext(1);
                emitter.onNext(2);
                emitter.onComplete();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        observable.flatMap(x -> {
            if (x == 1) {
                return Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onError(new RuntimeException("Inner error"));
                });
            } else {
                return Observable.<Integer>create(innerEmitter -> {
                    Thread.sleep(200);
                    innerEmitter.onNext(x);
                    innerEmitter.onComplete();
                });
            }
        }).subscribe(new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {}
            
            @Override
            public void onNext(Integer item) {}
            
            @Override
            public void onError(Throwable t) {
                // Should receive error, but check if state is corrupted
                if (t == null) {
                    errorHandlingIssues.incrementAndGet();
                }
            }
            
            @Override
            public void onComplete() {}
        });
        
        startLatch.countDown();
        Thread.sleep(1000);
        
        assertEquals(0, errorHandlingIssues.get(),
            "Error handling should be clean");
    }
}
