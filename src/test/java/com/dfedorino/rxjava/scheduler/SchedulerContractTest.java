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

class SchedulerContractTest {

    static Stream<Scheduler> schedulerProvider() {
        return Stream.of(new IOThreadScheduler(), new ComputationScheduler(), new SingleThreadScheduler());
    }

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("выполняет задачу до shutdown")
    void shouldExecuteTask(Scheduler scheduler) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        scheduler.execute(latch::countDown);
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        scheduler.shutdown();
    }

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("выбрасывает исключение после shutdown")
    void shouldThrowAfterShutdown(Scheduler scheduler) {
        scheduler.shutdown();
        assertThatThrownBy(() -> scheduler.execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
    }

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("сообщает о состоянии shutdown")
    void shouldReportShutdownState(Scheduler scheduler) {
        assertThat(scheduler.isShutdown()).isFalse();
        scheduler.shutdown();
        assertThat(scheduler.isShutdown()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("schedulerProvider")
    @DisplayName("обрабатывает многократный shutdown")
    void shouldHandleMultipleShutdowns(Scheduler scheduler) {
        scheduler.shutdown();
        assertThatNoException().isThrownBy(scheduler::shutdown);
    }
}
