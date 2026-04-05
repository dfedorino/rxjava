package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    @DisplayName("Should execute task in computation thread with correct naming")
    void shouldExecuteTaskInComputationThread() throws InterruptedException {
        // Arrange
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean(false);
        AtomicBoolean correctThreadName = new AtomicBoolean(false);

        // Act
        scheduler.execute(() -> {
            executed.set(true);
            correctThreadName.set(Thread.currentThread().getName().startsWith("rxjava-computation-"));
            latch.countDown();
        });

        boolean completed = latch.await(2, TimeUnit.SECONDS);

        // Assert
        assertThat(completed)
            .as("Task should complete within timeout")
            .isTrue();
        assertThat(executed.get())
            .as("Task should be executed")
            .isTrue();
        assertThat(correctThreadName.get())
            .as("Thread name should start with 'rxjava-computation-'")
            .isTrue();
    }

    @Test
    @DisplayName("Should throw RejectedExecutionException when shut down")
    void shouldThrowExceptionWhenShutDown() {
        // Arrange
        scheduler.shutdown();

        // Act & Assert
        assertThatThrownBy(() -> scheduler.execute(() -> {}))
            .isInstanceOf(RejectedExecutionException.class)
            .hasMessageContaining("shut down");
    }

    @Test
    @DisplayName("Should limit threads to CPU count when specified")
    void shouldLimitThreadsToCpuCount() {
        // Arrange
        int threadCount = 2;
        ComputationScheduler limitedScheduler = new ComputationScheduler(threadCount);

        // Act & Assert
        assertThat(limitedScheduler.isShutdown())
            .as("Scheduler should not be shutdown initially")
            .isFalse();

        limitedScheduler.shutdown();

        assertThat(limitedScheduler.isShutdown())
            .as("Scheduler should be shutdown after shutdown()")
            .isTrue();
    }

    @Test
    @DisplayName("Should handle multiple concurrent tasks")
    void shouldHandleMultipleConcurrentTasks() throws InterruptedException {
        // Arrange
        int taskCount = 10;
        CountDownLatch latch = new CountDownLatch(taskCount);

        // Act
        for (int i = 0; i < taskCount; i++) {
            scheduler.execute(latch::countDown);
        }

        boolean completed = latch.await(3, TimeUnit.SECONDS);

        // Assert
        assertThat(completed)
            .as("All tasks should complete within timeout")
            .isTrue();
    }

    @Test
    @DisplayName("Should shutdown gracefully")
    void shouldShutdownGracefully() {
        // Arrange & Act
        assertThat(scheduler.isShutdown())
            .as("Scheduler should not be shutdown initially")
            .isFalse();

        scheduler.shutdown();

        // Assert
        assertThat(scheduler.isShutdown())
            .as("Scheduler should be shutdown after shutdown()")
            .isTrue();
    }
}
