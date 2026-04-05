package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    @DisplayName("выполняет задачу в потоке вычислений")
    void shouldExecuteTaskInComputationThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean();
        AtomicBoolean correctThread = new AtomicBoolean();

        scheduler.execute(() -> {
            executed.set(true);
            correctThread.set(Thread.currentThread().getName().startsWith("rxjava-computation-"));
            latch.countDown();
        });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(executed.get()).isTrue();
        assertThat(correctThread.get()).isTrue();
    }

    @Test
    @DisplayName("выбрасывает исключение после shutdown")
    void shouldThrowAfterShutdown() {
        scheduler.shutdown();
        assertThatThrownBy(() -> scheduler.execute(() -> {
        }))
                .isInstanceOf(RejectedExecutionException.class)
                .hasMessageContaining("shut down");
    }

    @Test
    @DisplayName("ограничивает потоки до CPU count")
    void shouldLimitThreads() {
        ComputationScheduler limited = new ComputationScheduler(2);
        assertThat(limited.isShutdown()).isFalse();
        limited.shutdown();
        assertThat(limited.isShutdown()).isTrue();
    }

    @Test
    @DisplayName("обрабатывает конкурентные задачи")
    void shouldHandleConcurrentTasks() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) scheduler.execute(latch::countDown);
        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("корректно останавливается")
    void shouldShutdownGracefully() {
        assertThat(scheduler.isShutdown()).isFalse();
        scheduler.shutdown();
        assertThat(scheduler.isShutdown()).isTrue();
    }

    @Test
    @DisplayName("использует кол-во потоков по кол-ву ядер")
    void shouldUseExactlyCpuCoresThreads() throws InterruptedException {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        CountDownLatch finishLatch = new CountDownLatch(1);
        CountDownLatch allRunningLatch = new CountDownLatch(cpuCores);
        List<String> threadNames = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < cpuCores; i++) {
            scheduler.execute(() -> {
                threadNames.add(Thread.currentThread().getName());
                allRunningLatch.countDown();
                try {
                    finishLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        assertThat(allRunningLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threadNames).hasSize(cpuCores);
        long distinctThreads = threadNames.stream().distinct().count();
        assertThat(distinctThreads).isEqualTo(cpuCores);
        finishLatch.countDown();
    }
}
