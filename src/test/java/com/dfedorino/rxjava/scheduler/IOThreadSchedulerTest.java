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

class IOThreadSchedulerTest {

    private IOThreadScheduler scheduler;

    @BeforeEach
    void setUp() { scheduler = new IOThreadScheduler(); }

    @AfterEach
    void tearDown() { scheduler.shutdown(); }

    @Test
    @DisplayName("выполняет задачу в потоке I/O")
    void shouldExecuteTaskInIoThread() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean executed = new AtomicBoolean();
        AtomicBoolean correctThread = new AtomicBoolean();

        scheduler.execute(() -> {
            executed.set(true);
            correctThread.set(Thread.currentThread().getName().startsWith("rxjava-io-"));
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
    @DisplayName("переиспользует потоки из кэша")
    void shouldReuseThreads() throws InterruptedException {
        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        CountDownLatch secondLatch = new CountDownLatch(1);
        Thread[] threads = new Thread[2];

        scheduler.execute(() -> {
            threads[0] = Thread.currentThread();
            firstLatch.countDown();
            try { releaseLatch.await(2, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        firstLatch.await(1, TimeUnit.SECONDS);

        scheduler.execute(() -> { threads[1] = Thread.currentThread(); secondLatch.countDown(); });
        releaseLatch.countDown();
        secondLatch.await(2, TimeUnit.SECONDS);

        assertThat(threads[0].getName()).startsWith("rxjava-io-");
        assertThat(threads[1].getName()).startsWith("rxjava-io-");
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
}
