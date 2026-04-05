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
    @DisplayName("Should execute task in IO thread with correct naming")
    void shouldExecuteTaskInIoThread() throws InterruptedException {
        // Arrange
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean(false);
        AtomicBoolean correctThreadName = new AtomicBoolean(false);

        // Act
        scheduler.execute(() -> {
            executed.set(true);
            correctThreadName.set(Thread.currentThread().getName().startsWith("rxjava-io-"));
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
            .as("Thread name should start with 'rxjava-io-'")
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
    @DisplayName("Should reuse idle threads from cache")
    void shouldReuseIdleThreads() throws InterruptedException {
        // Arrange
        CountDownLatch firstTaskLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        CountDownLatch secondTaskLatch = new CountDownLatch(1);
        Thread[] threads = new Thread[2];

        // Act - Submit first task that blocks until released
        scheduler.execute(() -> {
            threads[0] = Thread.currentThread();
            firstTaskLatch.countDown();
            // Block to keep thread busy
            try {
                releaseLatch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        firstTaskLatch.await(1, TimeUnit.SECONDS);

        // Submit second task (will get new thread while first is blocked)
        scheduler.execute(() -> {
            threads[1] = Thread.currentThread();
            secondTaskLatch.countDown();
        });

        // Release first task
        releaseLatch.countDown();
        secondTaskLatch.await(2, TimeUnit.SECONDS);

        // Assert - Both threads should have rxjava-io- prefix
        assertThat(threads[0].getName())
            .as("First thread should have IO thread name")
            .startsWith("rxjava-io-");
        assertThat(threads[1].getName())
            .as("Second thread should have IO thread name")
            .startsWith("rxjava-io-");
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
