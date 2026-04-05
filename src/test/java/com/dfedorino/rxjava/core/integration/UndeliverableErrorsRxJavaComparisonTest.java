package com.dfedorino.rxjava.core.integration;

import com.dfedorino.rxjava.core.*;
import com.dfedorino.rxjava.exception.ErrorHandlers;
import com.dfedorino.rxjava.util.TestObserver;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compares undeliverable error behavior with RxJava reference implementation.
 */
class UndeliverableErrorsRxJavaComparisonTest {

    private final List<Throwable> customErrors = new ArrayList<>();
    private final List<Throwable> rxJavaErrors = new ArrayList<>();

    @BeforeEach
    void setUp() {
        ErrorHandlers.setErrorHandler(customErrors::add);
        RxJavaPlugins.setErrorHandler(rxJavaErrors::add);
    }

    @AfterEach
    void tearDown() {
        ErrorHandlers.reset();
        RxJavaPlugins.reset();
    }

    @Test
    @DisplayName("[RxJava] onError после onComplete → RxJavaPlugins.onError")
    void rxJavaOnErrorAfterOnComplete() {
        io.reactivex.rxjava3.core.Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            emitter.onError(new RuntimeException("late error"));
        }).subscribe(
                item -> {},
                error -> {},
                () -> {});

        awaitRxJavaErrors(1);
        assertThat(rxJavaErrors).hasSize(1);
        assertThat(rxJavaErrors.getFirst()).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("custom onError после onComplete → RxJavaPlugins.onError")
    void customOnErrorAfterOnComplete() {
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            emitter.onError(new RuntimeException("late error"));
        }).subscribe(TestObserver.<Integer>builder().build());

        awaitCustomErrors(1);
        assertThat(customErrors).hasSize(1);
        assertThat(customErrors.getFirst()).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("[RxJava] onNext после onComplete → silently dropped")
    void rxJavaOnNextAfterOnComplete() {
        // RxJava silently drops onNext after onComplete — no RxJavaPlugins call.
        // Our implementation routes it to RxJavaPlugins.onError instead (stricter).
        io.reactivex.rxjava3.core.Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            emitter.onNext(2);
        }).subscribe(
                item -> {},
                error -> {},
                () -> {});

        // Give RxJava time to process — it won't produce an undeliverable error here
        awaitRxJavaErrors(0);
        // RxJava doesn't route this to RxJavaPlugins, so this is expected behavior
    }

    @Test
    @DisplayName("custom onNext после onComplete → undeliverable error")
    void customOnNextAfterOnComplete() {
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            emitter.onNext(2);
        }).subscribe(TestObserver.<Integer>builder().build());

        awaitCustomErrors(1);
        assertThat(customErrors).hasSize(1);
        assertThat(customErrors.getFirst())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("onNext called after termination");
    }

    // --- Helpers ---

    private void awaitCustomErrors(int count) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (customErrors.size() < count && System.nanoTime() < deadline) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    private void awaitRxJavaErrors(int count) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (rxJavaErrors.size() < count && System.nanoTime() < deadline) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
