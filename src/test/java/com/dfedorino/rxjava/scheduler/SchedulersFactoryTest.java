package com.dfedorino.rxjava.scheduler;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulersFactoryTest {

    @Test
    @DisplayName("io() возвращает ненулевой singleton")
    void shouldReturnNonNullScheduler_io() {
        Scheduler scheduler = Schedulers.io();
        assertThat(scheduler).isNotNull();
        assertThat(scheduler.isShutdown()).isFalse();
        scheduler.shutdown();
    }

    @Test
    @DisplayName("computation() возвращает ненулевой singleton")
    void shouldReturnNonNullScheduler_computation() {
        Scheduler scheduler = Schedulers.computation();
        assertThat(scheduler).isNotNull();
        assertThat(scheduler.isShutdown()).isFalse();
        scheduler.shutdown();
    }

    @Test
    @DisplayName("single() возвращает ненулевой singleton")
    void shouldReturnNonNullScheduler_single() {
        Scheduler scheduler = Schedulers.single();
        assertThat(scheduler).isNotNull();
        assertThat(scheduler.isShutdown()).isFalse();
        scheduler.shutdown();
    }

    @Test
    @DisplayName("io() всегда возвращает один экземпляр")
    void shouldReturnSameInstance_io() {
        Scheduler first = Schedulers.io();
        Scheduler second = Schedulers.io();
        assertThat(first).isSameAs(second);
    }

    @Test
    @DisplayName("computation() всегда возвращает один экземпляр")
    void shouldReturnSameInstance_computation() {
        Scheduler first = Schedulers.computation();
        Scheduler second = Schedulers.computation();
        assertThat(first).isSameAs(second);
    }

    @Test
    @DisplayName("single() всегда возвращает один экземпляр")
    void shouldReturnSameInstance_single() {
        Scheduler first = Schedulers.single();
        Scheduler second = Schedulers.single();
        assertThat(first).isSameAs(second);
    }
}
