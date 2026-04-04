package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Scheduler Interface Tests")
class SchedulerTest {

    @Test
    @DisplayName("Should throw RejectedExecutionException after shutdown")
    void shouldThrowExceptionAfterShutdown() {
        Scheduler scheduler = new IOThreadScheduler();
        scheduler.shutdown();

        assertThrows(RejectedExecutionException.class, () -> scheduler.execute(() -> {}));
    }

    @Test
    @DisplayName("Should report isShutdown as true after shutdown()")
    void shouldReportShutdownState() {
        Scheduler scheduler = new IOThreadScheduler();
        assertFalse(scheduler.isShutdown());

        scheduler.shutdown();
        assertTrue(scheduler.isShutdown());
    }

    @Test
    @DisplayName("Should execute task successfully before shutdown")
    void shouldExecuteTaskBeforeShutdown() throws InterruptedException {
        Scheduler scheduler = new SingleThreadScheduler();
        CountDownLatch latch = new CountDownLatch(1);

        scheduler.execute(latch::countDown);
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        scheduler.shutdown();
    }

    @Test
    @DisplayName("Should handle multiple shutdown() calls gracefully")
    void shouldHandleMultipleShutdownCalls() {
        Scheduler scheduler = new ComputationScheduler();
        assertDoesNotThrow(() -> {
            scheduler.shutdown();
            scheduler.shutdown();
        });
    }
}
