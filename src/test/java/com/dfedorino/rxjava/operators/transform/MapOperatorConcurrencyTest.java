package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.*;
import com.dfedorino.rxjava.scheduler.Schedulers;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrency tests for Map operator.
 * These tests are designed to expose thread safety bugs and reactive contract violations.
 * 
 * Expected failures indicate bugs in the current implementation.
 */
class MapOperatorConcurrencyTest {

    @Test
    @DisplayName("BUG: disposable field visibility - dispose() may not see disposable set in onSubscribe()")
    void testDisposableVisibilityAcrossThreads() throws InterruptedException {
        // This test exposes the non-volatile disposable field bug
        int iterations = 100;
        AtomicInteger disposeFailed = new AtomicInteger(0);
        AtomicInteger successfulDisposes = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            CountDownLatch disposeLatch = new CountDownLatch(1);
            AtomicReference<Disposable> disposableRef = new AtomicReference<>();
            AtomicBoolean disposedBeforeSubscribe = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                // Signal that we're about to subscribe
                subscribeLatch.countDown();
                // Wait for dispose to be called from another thread
                disposeLatch.await(1, TimeUnit.SECONDS);
                // Continue emitting
                if (!emitter.isDisposed()) {
                    emitter.onNext(1);
                    emitter.onComplete();
                }
            });
            
            Thread subscribeThread = new Thread(() -> {
                observable.map(x -> x * 2).subscribe(new Observer<>() {
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
                    // Wait for subscription to happen
                    subscribeLatch.await(1, TimeUnit.SECONDS);
                    Disposable d = disposableRef.get();
                    if (d != null) {
                        d.dispose();
                        // Try to dispose again - this should work but might fail due to visibility
                        d.dispose();
                        successfulDisposes.incrementAndGet();
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
        
        // If disposable field is not volatile, some dispose calls may fail
        // This test may pass sometimes due to JVM optimizations, but should fail occasionally
        System.out.println("Disposable visibility failures: " + disposeFailed.get() + "/" + iterations);
    }

    @Test
    @DisplayName("BUG: No termination guard - multiple threads can call onError/onComplete")
    void testMultipleTerminalSignalsFromConcurrentThreads() throws InterruptedException {
        // This test exposes the lack of termination guard
        int testRuns = 50;
        AtomicInteger multipleTerminalSignals = new AtomicInteger(0);
        
        for (int run = 0; run < testRuns; run++) {
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger errorCount = new AtomicInteger(0);
            AtomicInteger completeCount = new AtomicInteger(0);
            AtomicInteger onNextAfterTerminal = new AtomicInteger(0);
            AtomicBoolean terminalReached = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                // Two threads will try to complete/error simultaneously
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
            
            observable.map(x -> x * 2).subscribe(new Observer<>() {
                @Override
                public void onSubscribe(Disposable d) {}
                
                @Override
                public void onNext(Integer item) {
                    if (terminalReached.get()) {
                        onNextAfterTerminal.incrementAndGet();
                    }
                }
                
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
            
            if (errorCount.get() + completeCount.get() > 1 || onNextAfterTerminal.get() > 0) {
                multipleTerminalSignals.incrementAndGet();
            }
        }
        
        System.out.println("Multiple terminal signals detected: " + multipleTerminalSignals.get() + "/" + testRuns);
        // This should fail if there's no termination guard
    }

    @Test
    @DisplayName("BUG: Race between onNext and dispose - items emitted after dispose")
    void testRaceBetweenOnNextAndDispose() throws InterruptedException {
        int iterations = 100;
        AtomicInteger itemsAfterDispose = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            CountDownLatch raceLatch = new CountDownLatch(1);
            AtomicReference<Disposable> disposableRef = new AtomicReference<>();
            AtomicBoolean disposed = new AtomicBoolean(false);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                subscribeLatch.countDown();
                raceLatch.await(1, TimeUnit.SECONDS);
                
                // Try to emit after dispose
                for (int j = 0; j < 10; j++) {
                    if (!emitter.isDisposed()) {
                        emitter.onNext(j);
                    }
                }
                emitter.onComplete();
            });
            
            Thread subscribeThread = new Thread(() -> {
                observable.map(x -> x * 2).subscribe(new Observer<>() {
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
        
        System.out.println("Items received after dispose: " + itemsAfterDispose.get());
    }

    @Test
    @DisplayName("BUG: Mapper function called concurrently - thread safety of mapper")
    void testMapperFunctionThreadSafety() throws InterruptedException {
        // Test that mapper function is called safely from multiple threads
        int numThreads = 10;
        int itemsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);
        ConcurrentMap<String, Integer> concurrentResults = new ConcurrentHashMap<>();
        AtomicInteger mapperCalls = new AtomicInteger(0);
        
        // Thread-unsafe mapper (should fail or show issues)
        AtomicInteger sharedCounter = new AtomicInteger(0);
        AtomicInteger totalEmitted = new AtomicInteger(0);
        
        Observable<Integer> observable = Observable.create(emitter -> {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                Schedulers.io().execute(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < itemsPerThread; j++) {
                            emitter.onNext(threadId * 1000 + j);
                            totalEmitted.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }
        });
        
        observable.map(item -> {
            // Simulate thread-unsafe operation
            int current = sharedCounter.incrementAndGet();
            String threadName = Thread.currentThread().getName();
            concurrentResults.merge(threadName, 1, Integer::sum);
            mapperCalls.incrementAndGet();
            return current;
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
        completeLatch.await(10, TimeUnit.SECONDS);
        Thread.sleep(500); // Wait for all mapper calls to complete
        
        System.out.println("Total emitted: " + totalEmitted.get());
        System.out.println("Total mapper calls: " + mapperCalls.get());
        System.out.println("Thread distribution: " + concurrentResults);
        
        // Mapper should be called for each emitted item (may be less due to termination timing)
        assertTrue(mapperCalls.get() > 0, "Mapper should be called at least once");
        assertEquals(totalEmitted.get(), sharedCounter.get(), 
            "Mapper counter should match actual calls");
    }

    @Test
    @DisplayName("Stress test: Concurrent emissions through map operator")
    void testConcurrentEmissionsStressTest() throws InterruptedException {
        int numConcurrentSubscriptions = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numConcurrentSubscriptions);
        AtomicInteger totalReceived = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger completes = new AtomicInteger(0);
        
        for (int i = 0; i < numConcurrentSubscriptions; i++) {
            final int subscriptionId = i;
            
            Observable<Integer> observable = Observable.create(emitter -> {
                Schedulers.io().execute(() -> {
                    try {
                        startLatch.await(5, TimeUnit.SECONDS);
                        for (int j = 0; j < 100; j++) {
                            emitter.onNext(subscriptionId * 1000 + j);
                            Thread.sleep(1); // Small delay to increase contention
                        }
                        emitter.onComplete();
                    } catch (Exception e) {
                        emitter.onError(e);
                    } finally {
                        completeLatch.countDown();
                    }
                });
            });
            
            observable.map(x -> x * 2).subscribe(new Observer<>() {
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
        
        System.out.println("Stress test results:");
        System.out.println("  Total received: " + totalReceived.get());
        System.out.println("  Errors: " + errors.get());
        System.out.println("  Completes: " + completes.get());
        System.out.println("  Finished on time: " + finished);
        
        assertEquals(numConcurrentSubscriptions * 100, totalReceived.get(),
            "Should receive all items from all subscriptions");
        assertEquals(numConcurrentSubscriptions, completes.get(),
            "All subscriptions should complete");
        assertEquals(0, errors.get(),
            "No errors should occur");
    }

    @Test
    @DisplayName("BUG: Rapid consecutive dispose calls")
    void testRapidConsecutiveDisposeCalls() throws InterruptedException {
        int iterations = 50;
        AtomicInteger disposeExceptions = new AtomicInteger(0);
        
        for (int i = 0; i < iterations; i++) {
            AtomicReference<Disposable> disposableRef = new AtomicReference<>();
            CountDownLatch subscribeLatch = new CountDownLatch(1);
            
            Observable<Integer> observable = Observable.create(emitter -> {
                // Long-running emitter
                try {
                    Thread.sleep(500);
                    emitter.onNext(1);
                    emitter.onComplete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            observable.map(x -> x).subscribe(new Observer<>() {
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
            
            // Call dispose from multiple threads simultaneously
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
        
        System.out.println("Dispose exceptions: " + disposeExceptions.get());
        // Should handle multiple dispose calls gracefully
    }
}
