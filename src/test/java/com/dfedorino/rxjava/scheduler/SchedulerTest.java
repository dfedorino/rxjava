package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests specific to the Scheduler interface contract.
 * For implementation-specific tests, see individual scheduler test classes.
 */
@DisplayName("Scheduler Interface Tests")
class SchedulerTest {

    @Test
    @DisplayName("Should execute task and verify completion")
    void shouldExecuteTaskAndVerifyCompletion() throws InterruptedException {
        // Arrange
        Scheduler scheduler = new SingleThreadScheduler();
        CountDownLatch latch = new CountDownLatch(1);

        // Act
        scheduler.execute(latch::countDown);
        boolean completed = latch.await(1, TimeUnit.SECONDS);

        // Assert
        assertThat(completed)
            .as("Task should complete within timeout")
            .isTrue();

        scheduler.shutdown();
    }
}
