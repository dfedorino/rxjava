package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.exception.ErrorHandlers;
import com.dfedorino.rxjava.operators.predicate.FilterObserver;
import com.dfedorino.rxjava.operators.transform.MapObserver;
import com.dfedorino.rxjava.util.NoOpDisposable;
import com.dfedorino.rxjava.util.TestObserver;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class UndeliverableErrorsTest {

    private List<Throwable> capturedErrors;

    @BeforeEach
    void setUp() {
        capturedErrors = new ArrayList<>();
        ErrorHandlers.setErrorHandler(capturedErrors::add);
    }

    @AfterEach
    void tearDown() {
        ErrorHandlers.reset();
    }

    // --- CreateEmitter ---

    @Test
    @DisplayName("onError после onComplete")
    void onErrorAfterOnComplete() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            emitter.onError(new RuntimeException("late error"));
            latch.countDown();
        }).subscribe(TestObserver.<Integer>builder().build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedErrors.getFirst())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("late error");
    }

    @Test
    @DisplayName("onError после dispose")
    void onErrorAfterDispose() {
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
        Observable.<Integer>create(emitter -> {
            emitterRef.set(emitter);
            emitter.onNext(1);
            emitter.onComplete();  // terminate first
        }).subscribe(TestObserver.<Integer>builder()
                .onCompleteAction(() -> emitterRef.get().dispose())
                .build());

        assertThat(emitterRef.get().isDisposed()).isTrue();
        // Now both terminated and disposed — error is undeliverable
        emitterRef.get().onError(new RuntimeException("late error"));

        assertThat(capturedErrors.getFirst())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("late error");
    }

    @Test
    @DisplayName("onNext после onComplete")
    void onNextAfterOnComplete() {
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            emitter.onNext(2);
        }).subscribe(TestObserver.<Integer>builder().build());

        assertThat(capturedErrors.getFirst())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("onNext called after termination");
    }

    @Test
    @DisplayName("onNext после dispose")
    void onNextAfterDispose() {
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();
        Observable.<Integer>create(emitter -> {
            emitterRef.set(emitter);
            emitter.onNext(1);
        }).subscribe(TestObserver.<Integer>builder()
                .onNextAction(item -> emitterRef.get().dispose())
                .build());

        emitterRef.get().onNext(2);

        assertThat(capturedErrors.getFirst())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("onNext called after termination or disposal");
    }

    // --- MapObserver ---

    @Test
    @DisplayName("map onError после onComplete")
    void mapOnErrorAfterOnComplete() {
        MapObserver<String, String> mapObs = new MapObserver<>(TestObserver.<String>builder().build(), s -> s);

        mapObs.onSubscribe(new NoOpDisposable());

        mapObs.onComplete();
        mapObs.onError(new RuntimeException("test"));

        assertThat(capturedErrors)
                .hasSize(1)
                .first(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("test");
    }

    @Test
    @DisplayName("map onComplete после onComplete")
    void mapOnCompleteAfterOnComplete() {
        MapObserver<String, String> mapObs = new MapObserver<>(TestObserver.<String>builder().build(), s -> s);

        mapObs.onSubscribe(new NoOpDisposable());

        mapObs.onComplete();
        mapObs.onComplete();

        assertThat(capturedErrors)
                .hasSize(1)
                .first(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("onComplete called after termination");
    }

    // --- FilterObserver ---

    @Test
    @DisplayName("filter onError после onComplete")
    void filterOnErrorAfterOnComplete() {
        FilterObserver<Integer> filterObs = new FilterObserver<>(TestObserver.<Integer>builder().build(), x -> true);

        filterObs.onSubscribe(new NoOpDisposable());

        filterObs.onComplete();
        filterObs.onError(new RuntimeException("test"));

        assertThat(capturedErrors).hasSize(1)
                .first(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("test");
    }

    @Test
    @DisplayName("filter onComplete после onComplete")
    void filterOnCompleteAfterOnComplete() {
        FilterObserver<Integer> filterObs = new FilterObserver<>(TestObserver.<Integer>builder().build(), x -> true);

        filterObs.onSubscribe(new NoOpDisposable());

        filterObs.onComplete();
        filterObs.onComplete();

        assertThat(capturedErrors).hasSize(1)
                .first(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("onComplete called after termination");
    }
}
