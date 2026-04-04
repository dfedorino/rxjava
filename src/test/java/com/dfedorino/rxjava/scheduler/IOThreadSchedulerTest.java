package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("IOThreadScheduler Tests")
class IOThreadSchedulerTest {

    private IOThreadScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new IOThreadScheduler();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    @DisplayName("Should execute task in IO thread")
    void shouldExecuteTaskInIoThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean(false);

        scheduler.execute(() -> {
            executed.set(true);
            assertTrue(Thread.currentThread().getName().startsWith("rxjava-io-"));
            latch.countDown();
        });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertTrue(executed.get());
    }

    @Test
    @DisplayName("Should throw RejectedExecutionException when shut down")
    void shouldThrowExceptionWhenShutDown() {
        scheduler.shutdown();
        assertThrows(RejectedExecutionException.class, () -> scheduler.execute(() -> {}));
    }

    @Test
    @DisplayName("Should reuse idle threads")
    void shouldReuseIdleThreads() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean sameThread = new AtomicBoolean(false);
        Thread[] threads = new Thread[2];

        scheduler.execute(() -> {
            threads[0] = Thread.currentThread();
            latch.countDown();
        });

        latch.await(1, TimeUnit.SECONDS);

        scheduler.execute(() -> {
            threads[1] = Thread.currentThread();
            latch.countDown();
        });

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(threads[0], threads[1]);
    }

    @Test
    @DisplayName("Should handle multiple concurrent tasks")
    void shouldHandleMultipleConcurrentTasks() throws InterruptedException {
        int taskCount = 10;
        CountDownLatch latch = new CountDownLatch(taskCount);

        for (int i = 0; i < taskCount; i++) {
            scheduler.execute(latch::countDown);
        }

        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Should shutdown gracefully")
    void shouldShutdownGracefully() {
        assertFalse(scheduler.isShutdown());
        scheduler.shutdown();
        assertTrue(scheduler.isShutdown());
    }
}
