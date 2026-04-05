package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parameterized tests for all Scheduler implementations.
 * Tests common behavior defined by the Scheduler interface.
 */
@DisplayName("Scheduler Implementations - Common Behavior")
class SchedulerContractTest {

    static Stream<Scheduler> schedulerProvider() {
        return Stream.of(
            new IOThreadScheduler(),
            new ComputationScheduler(),
            new SingleThreadScheduler()
        );
    }

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("Should execute task successfully before shutdown")
    void shouldExecuteTaskBeforeShutdown(Scheduler scheduler) throws InterruptedException {
        // Arrange
        CountDownLatch latch = new CountDownLatch(1);

        // Act
        scheduler.execute(latch::countDown);
        boolean completed = latch.await(2, TimeUnit.SECONDS);

        // Assert
        assertThat(completed)
            .as("Task should complete within timeout")
            .isTrue();

        scheduler.shutdown();
    }

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("Should throw RejectedExecutionException after shutdown")
    void shouldThrowExceptionAfterShutdown(Scheduler scheduler) {
        // Arrange
        scheduler.shutdown();

        // Act & Assert
        assertThatThrownBy(() -> scheduler.execute(() -> {}))
            .isInstanceOf(RejectedExecutionException.class);
    }

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("Should report isShutdown as true after shutdown()")
    void shouldReportShutdownState(Scheduler scheduler) {
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

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("Should handle multiple shutdown() calls gracefully")
    void shouldHandleMultipleShutdownCalls(Scheduler scheduler) {
        // Arrange
        scheduler.shutdown();

        // Act & Assert
        assertThatNoException()
            .isThrownBy(scheduler::shutdown);
    }
}
