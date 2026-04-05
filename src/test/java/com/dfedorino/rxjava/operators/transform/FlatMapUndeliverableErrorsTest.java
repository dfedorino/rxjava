package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.*;
import com.dfedorino.rxjava.exception.ErrorHandlers;
import com.dfedorino.rxjava.util.NoOpDisposable;
import com.dfedorino.rxjava.util.TestObserver;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMapUndeliverableErrorsTest {

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

    @Test
    @DisplayName("flatMap onError после dispose")
    void flatMapOnErrorAfterDispose() {
        FlatMapObserver<Integer, String> flatMapObs = new FlatMapObserver<>(TestObserver.<String>builder().build(),
                i -> Observable.create(ObservableEmitter::onComplete));

        flatMapObs.onSubscribe(new NoOpDisposable());

        flatMapObs.dispose();
        flatMapObs.onError(new RuntimeException("late error"));

        assertThat(capturedErrors)
                .hasSize(1)
                .first(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("late error");
    }

    @Test
    @DisplayName("flatMap innerError после dispose")
    void flatMapInnerErrorAfterDispose() {
        FlatMapObserver<Integer, String> flatMapObs = new FlatMapObserver<>(TestObserver.<String>builder().build(),
                i -> Observable.create(ObservableEmitter::onComplete));

        flatMapObs.onSubscribe(new NoOpDisposable());

        flatMapObs.dispose();
        flatMapObs.innerError(new RuntimeException("inner late error"));

        assertThat(capturedErrors)
                .hasSize(1)
                .first(InstanceOfAssertFactories.THROWABLE)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("inner late error");
    }
}
