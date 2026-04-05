package com.dfedorino.rxjava.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SingleThreadSchedulerTest {

    private SingleThreadScheduler scheduler;

    @BeforeEach
    void setUp() { scheduler = new SingleThreadScheduler(); }

    @AfterEach
    void tearDown() { scheduler.shutdown(); }

    @Test
    @DisplayName("выполняет задачу в одиночном потоке")
    void shouldExecuteTaskInSingleThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean();
        AtomicBoolean correctThread = new AtomicBoolean();

        scheduler.execute(() -> {
            executed.set(true);
            correctThread.set("rxjava-single".equals(Thread.currentThread().getName()));
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
        assertThatThrownBy(() -> scheduler.execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class)
                .hasMessageContaining("shut down");
    }

    @Test
    @DisplayName("выполняет задачи последовательно в одном потоке")
    void shouldExecuteSequentially() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        Thread[] threads = new Thread[3];

        scheduler.execute(() -> { threads[0] = Thread.currentThread(); latch.countDown(); });
        scheduler.execute(() -> { threads[1] = Thread.currentThread(); latch.countDown(); });
        scheduler.execute(() -> { threads[2] = Thread.currentThread(); latch.countDown(); });

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(threads[1]).isSameAs(threads[0]);
        assertThat(threads[2]).isSameAs(threads[0]);
    }

    @Test
    @DisplayName("сохраняет порядок выполнения")
    void shouldMaintainOrder() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger order = new AtomicInteger();
        int[] result = new int[3];

        scheduler.execute(() -> { result[0] = order.incrementAndGet(); latch.countDown(); });
        scheduler.execute(() -> { result[1] = order.incrementAndGet(); latch.countDown(); });
        scheduler.execute(() -> { result[2] = order.incrementAndGet(); latch.countDown(); });

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(result[0]).isEqualTo(1);
        assertThat(result[1]).isEqualTo(2);
        assertThat(result[2]).isEqualTo(3);
    }

    @Test
    @DisplayName("корректно останавливается")
    void shouldShutdownGracefully() {
        assertThat(scheduler.isShutdown()).isFalse();
        scheduler.shutdown();
        assertThat(scheduler.isShutdown()).isTrue();
    }

    @Test
    @DisplayName("все задачи выполняются на одном и том же потоке")
    void shouldExecuteAllTasksOnSameThread() throws InterruptedException {
        int taskCount = 5;
        CountDownLatch latch = new CountDownLatch(taskCount);
        Set<String> threadNames = new HashSet<>();

        for (int i = 0; i < taskCount; i++) {
            scheduler.execute(() -> {
                threadNames.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(threadNames)
                .hasSize(1)
                .first()
                .isEqualTo("rxjava-single");
    }
}
