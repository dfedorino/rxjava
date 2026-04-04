package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SingleThreadScheduler Tests")
class SingleThreadSchedulerTest {

    private SingleThreadScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SingleThreadScheduler();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    @DisplayName("Should execute task in single thread")
    void shouldExecuteTaskInSingleThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean(false);

        scheduler.execute(() -> {
            executed.set(true);
            assertEquals("rxjava-single", Thread.currentThread().getName());
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
    @DisplayName("Should execute tasks sequentially in same thread")
    void shouldExecuteTasksSequentially() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger threadCounter = new AtomicInteger(0);
        Thread[] threads = new Thread[3];

        scheduler.execute(() -> {
            threads[0] = Thread.currentThread();
            latch.countDown();
        });

        scheduler.execute(() -> {
            threads[1] = Thread.currentThread();
            latch.countDown();
        });

        scheduler.execute(() -> {
            threads[2] = Thread.currentThread();
            latch.countDown();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(threads[0], threads[1]);
        assertEquals(threads[1], threads[2]);
    }

    @Test
    @DisplayName("Should maintain task execution order")
    void shouldMaintainTaskOrder() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger executionOrder = new AtomicInteger(0);
        int[] order = new int[3];

        scheduler.execute(() -> {
            order[0] = executionOrder.incrementAndGet();
            latch.countDown();
        });

        scheduler.execute(() -> {
            order[1] = executionOrder.incrementAndGet();
            latch.countDown();
        });

        scheduler.execute(() -> {
            order[2] = executionOrder.incrementAndGet();
            latch.countDown();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(1, order[0]);
        assertEquals(2, order[1]);
        assertEquals(3, order[2]);
    }

    @Test
    @DisplayName("Should shutdown gracefully")
    void shouldShutdownGracefully() {
        assertFalse(scheduler.isShutdown());
        scheduler.shutdown();
        assertTrue(scheduler.isShutdown());
    }
}
