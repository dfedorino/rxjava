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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    @DisplayName("Should execute task in single thread with correct naming")
    void shouldExecuteTaskInSingleThread() throws InterruptedException {
        // Arrange
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean(false);
        AtomicBoolean correctThreadName = new AtomicBoolean(false);

        // Act
        scheduler.execute(() -> {
            executed.set(true);
            correctThreadName.set("rxjava-single".equals(Thread.currentThread().getName()));
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
            .as("Thread name should be 'rxjava-single'")
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
    @DisplayName("Should execute tasks sequentially in same thread")
    void shouldExecuteTasksSequentially() throws InterruptedException {
        // Arrange
        CountDownLatch latch = new CountDownLatch(3);
        Thread[] threads = new Thread[3];

        // Act
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

        boolean completed = latch.await(3, TimeUnit.SECONDS);

        // Assert
        assertThat(completed)
            .as("All tasks should complete within timeout")
            .isTrue();
        assertThat(threads[1])
            .as("Second task should run in same thread as first")
            .isSameAs(threads[0]);
        assertThat(threads[2])
            .as("Third task should run in same thread as first")
            .isSameAs(threads[0]);
    }

    @Test
    @DisplayName("Should maintain task execution order")
    void shouldMaintainTaskOrder() throws InterruptedException {
        // Arrange
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger executionOrder = new AtomicInteger(0);
        int[] order = new int[3];

        // Act
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

        boolean completed = latch.await(3, TimeUnit.SECONDS);

        // Assert
        assertThat(completed)
            .as("All tasks should complete within timeout")
            .isTrue();
        assertThat(order[0])
            .as("First task should execute first")
            .isEqualTo(1);
        assertThat(order[1])
            .as("Second task should execute second")
            .isEqualTo(2);
        assertThat(order[2])
            .as("Third task should execute third")
            .isEqualTo(3);
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
