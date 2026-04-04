package com.dfedorino.rxjava.operators.predicate;

import com.dfedorino.rxjava.core.*;
import com.dfedorino.rxjava.scheduler.IOThreadScheduler;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrency tests for Filter operator.
 * These tests are designed to expose thread safety bugs and reactive contract violations.
 * 
 * Expected failures indicate bugs in the current implementation.
 */
class FilterOperatorConcurrencyTest {

    private IOThreadScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new IOThreadScheduler();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    @DisplayName("BUG: disposable field visibility - same bug as MapObserver")
    void testDisposableVisibilityAcrossThreads() throws InterruptedException {
        int iterations = 100;
        AtomicInteger disposeFailed = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            CountDownLatch disposeLatch = new CountDownLatch(1);
            AtomicReference<Disposable> disposableRef = new AtomicReference<>();
            
            Observable<Integer> observable = Observable.create(emitter -> {
                subscribeLatch.countDown();
                disposeLatch.await(1, TimeUnit.SECONDS);
                if (!emitter.isDisposed()) {
                    emitter.onNext(1);
                    emitter.onComplete();
                }
            });
            
            Thread subscribeThread = new Thread(() -> {
                observable.filter(x -> x > 0).subscribe(new Observer<>() {
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
            });
            
            Thread disposeThread = new Thread(() -> {
                try {
                    subscribeLatch.await(1, TimeUnit.SECONDS);
                    Disposable d = disposableRef.get();
                    if (d != null) {
                        d.dispose();
                        d.dispose();
                    } else {
                        disposeFailed.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            disposeThread.start();
            subscribeThread.start();
            disposeThread.join(2000);
            subscribeThread.join(2000);
            disposeLatch.countDown();
        }
        
        System.out.println("Filter disposable visibility failures: " + disposeFailed.get() + "/" + iterations);
    }

    @Test
    @DisplayName("BUG: No termination guard - concurrent onError/onComplete")
    void testMultipleTerminalSignalsFromConcurrentThreads() throws InterruptedException {
        int testRuns = 50;
        AtomicInteger multipleTerminalSignals = new AtomicInteger(0);
        
        for (int run = 0; run < testRuns; run++) {
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger errorCount = new AtomicInteger(0);
            AtomicInteger completeCount = new AtomicInteger(0);
            AtomicBoolean terminalReached = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                Thread errorThread = new Thread(() -> {
                    try {
                        startLatch.await();
                        emitter.onError(new RuntimeException("Error from thread 1"));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                
                Thread completeThread = new Thread(() -> {
                    try {
                        startLatch.await();
                        emitter.onComplete();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                
                errorThread.start();
                completeThread.start();
            });
            
            observable.filter(x -> true).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {}
                
                @Override
                public void onNext(Integer item) {}
                
                @Override
                public void onError(Throwable t) {
                    if (terminalReached.getAndSet(true)) {
                        errorCount.incrementAndGet();
                    }
                }
                
                @Override
                public void onComplete() {
                    if (terminalReached.getAndSet(true)) {
                        completeCount.incrementAndGet();
                    }
                }
            });
            
            startLatch.countDown();
            Thread.sleep(200);
            
            if (errorCount.get() + completeCount.get() > 0) {
                multipleTerminalSignals.incrementAndGet();
            }
        }
        
        System.out.println("Filter multiple terminal signals: " + multipleTerminalSignals.get() + "/" + testRuns);
    }

    @Test
    @DisplayName("BUG: Predicate called after dispose")
    void testPredicateCalledAfterDispose() throws InterruptedException {
        int iterations = 100;
        AtomicInteger predicateAfterDispose = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            CountDownLatch raceLatch = new CountDownLatch(1);
            AtomicReference<Disposable> disposableRef = new AtomicReference<>();
            AtomicBoolean disposed = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                subscribeLatch.countDown();
                raceLatch.await(1, TimeUnit.SECONDS);
                
                for (int j = 0; j < 10; j++) {
                    if (!emitter.isDisposed()) {
                        emitter.onNext(j);
                    }
                }
                emitter.onComplete();
            });
            
            Thread subscribeThread = new Thread(() -> {
                observable.filter(x -> {
                    if (disposed.get()) {
                        predicateAfterDispose.incrementAndGet();
                    }
                    return true;
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
            });
            
            Thread disposeThread = new Thread(() -> {
                try {
                    subscribeLatch.await();
                    raceLatch.countDown();
                    Disposable d = disposableRef.get();
                    if (d != null) {
                        disposed.set(true);
                        d.dispose();
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
        
        System.out.println("Filter predicate calls after dispose: " + predicateAfterDispose.get());
    }

    @Test
    @DisplayName("BUG: Race between predicate evaluation and dispose")
    void testRaceBetweenPredicateEvaluationAndDispose() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger raceConditionItems = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            // Emit from multiple threads
            for (int i = 0; i < 10; i++) {
                final int value = i;
                scheduler.execute(() -> {
                    try {
                        startLatch.await();
                        emitter.onNext(value);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        });
        
        observable.filter(x -> {
            // Simulate slow predicate
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return x % 2 == 0;
        }).subscribe(new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
                disposableRef.set(d);
                subscribeLatch.countDown();
            }
            
            @Override
            public void onNext(Integer item) {
                // Try to dispose while predicate is being evaluated
                disposableRef.get().dispose();
                raceConditionItems.incrementAndGet();
            }
            
            @Override
            public void onError(Throwable t) {}
            
            @Override
            public void onComplete() {}
        });
        
        subscribeLatch.await(1, TimeUnit.SECONDS);
        startLatch.countDown();
        Thread.sleep(2000);
        
        System.out.println("Items processed during race: " + raceConditionItems.get());
    }

    @Test
    @DisplayName("Stress test: Concurrent filter operations")
    void testConcurrentFilterStressTest() throws InterruptedException {
        int numConcurrentSubscriptions = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numConcurrentSubscriptions);
        AtomicInteger totalReceived = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger completes = new AtomicInteger(0);
        
        for (int i = 0; i < numConcurrentSubscriptions; i++) {
            final int subscriptionId = i;
            
            Observable<Integer> observable = Observable.create(emitter -> {
                scheduler.execute(() -> {
                    try {
                        startLatch.await(5, TimeUnit.SECONDS);
                        for (int j = 0; j < 100; j++) {
                            emitter.onNext(subscriptionId * 1000 + j);
                            Thread.sleep(1);
                        }
                        emitter.onComplete();
                    } catch (Exception e) {
                        emitter.onError(e);
                    } finally {
                        completeLatch.countDown();
                    }
                });
            });
            
            observable.filter(x -> x % 2 == 0).subscribe(new Observer<>() {
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
        }
        
        startLatch.countDown();
        boolean finished = completeLatch.await(30, TimeUnit.SECONDS);
        
        System.out.println("Filter stress test results:");
        System.out.println("  Total received: " + totalReceived.get());
        System.out.println("  Errors: " + errors.get());
        System.out.println("  Completes: " + completes.get());
        System.out.println("  Finished on time: " + finished);
        
        // Should receive roughly half (even numbers)
        assertTrue(totalReceived.get() > 0, "Should receive some items");
        assertEquals(numConcurrentSubscriptions, completes.get(),
            "All subscriptions should complete");
    }

    @Test
    @DisplayName("BUG: Concurrent emissions with slow predicate")
    void testConcurrentEmissionsWithSlowPredicate() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger itemsReceived = new AtomicInteger(0);
        AtomicInteger predicateCalls = new AtomicInteger(0);
        AtomicBoolean predicateConcurrent = new AtomicBoolean(false);
        AtomicInteger concurrentPredicateCount = new AtomicInteger(0);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            for (int i = 0; i < 50; i++) {
                scheduler.execute(() -> {
                    try {
                        startLatch.await();
                        emitter.onNext(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        });
        
        observable.filter(x -> {
            int current = predicateCalls.incrementAndGet();
            if (current > 1) {
                predicateConcurrent.set(true);
            }
            concurrentPredicateCount.set(current);
            
            try {
                Thread.sleep(50); // Slow predicate
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            predicateCalls.decrementAndGet();
            return true;
        }).subscribe(new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {}
            
            @Override
            public void onNext(Integer item) {
                itemsReceived.incrementAndGet();
            }
            
            @Override
            public void onError(Throwable t) {}
            
            @Override
            public void onComplete() {}
        });
        
        startLatch.countDown();
        Thread.sleep(5000);
        
        System.out.println("Filter concurrent emissions:");
        System.out.println("  Items received: " + itemsReceived.get());
        System.out.println("  Max concurrent predicate calls: " + concurrentPredicateCount.get());
        System.out.println("  Was concurrent: " + predicateConcurrent.get());
        
        assertTrue(predicateConcurrent.get(), "Predicate should be called concurrently");
    }

    @Test
    @DisplayName("BUG: Rapid consecutive dispose calls on filter")
    void testRapidConsecutiveDisposeCalls() throws InterruptedException {
        int iterations = 50;
        AtomicInteger disposeExceptions = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            AtomicReference<Disposable> disposableRef = new AtomicReference<>();
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                try {
                    Thread.sleep(500);
                    emitter.onNext(1);
                    emitter.onComplete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            observable.filter(x -> true).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {
                    disposableRef.set(d);
                    subscribeLatch.countDown();
                }
                
                @Override
                public void onNext(Integer item) {}
                
                @Override
                public void onError(Throwable t) {}
                
                @Override
                public void onComplete() {}
            });
            
            subscribeLatch.await(1, TimeUnit.SECONDS);
            Disposable disposable = disposableRef.get();
            
            int numDisposeThreads = 10;
            CountDownLatch disposeStartLatch = new CountDownLatch(1);
            CountDownLatch disposeCompleteLatch = new CountDownLatch(numDisposeThreads);
            
            for (int j = 0; j < numDisposeThreads; j++) {
                new Thread(() -> {
                    try {
                        disposeStartLatch.await();
                        disposable.dispose();
                    } catch (Exception e) {
                        disposeExceptions.incrementAndGet();
                    } finally {
                        disposeCompleteLatch.countDown();
                    }
                }).start();
            }
            
            disposeStartLatch.countDown();
            disposeCompleteLatch.await(2, TimeUnit.SECONDS);
        }
        
        System.out.println("Filter dispose exceptions: " + disposeExceptions.get());
    }
}
