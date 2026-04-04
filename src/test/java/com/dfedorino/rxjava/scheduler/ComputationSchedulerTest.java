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

@DisplayName("ComputationScheduler Tests")
class ComputationSchedulerTest {

    private ComputationScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new ComputationScheduler();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    @DisplayName("Should execute task in computation thread")
    void shouldExecuteTaskInComputationThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean(false);

        scheduler.execute(() -> {
            executed.set(true);
            assertTrue(Thread.currentThread().getName().startsWith("rxjava-computation-"));
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
    @DisplayName("Should limit threads to CPU count")
    void shouldLimitThreadsToCpuCount() {
        int cpuCount = Runtime.getRuntime().availableProcessors();
        ComputationScheduler limitedScheduler = new ComputationScheduler(2);
        
        assertFalse(limitedScheduler.isShutdown());
        limitedScheduler.shutdown();
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
