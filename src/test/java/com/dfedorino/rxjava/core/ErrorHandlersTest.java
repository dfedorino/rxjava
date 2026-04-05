package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.exception.ErrorHandlers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorHandlersTest {

    private List<Throwable> capturedErrors;

    @BeforeEach
    void setUp() {
        ErrorHandlers.reset();
        capturedErrors = new ArrayList<>();
    }

    @Test
    @DisplayName("onError вызывает зарегистрированный обработчик")
    void shouldCallRegisteredHandler() {
        ErrorHandlers.setErrorHandler(capturedErrors::add);
        RuntimeException testError = new RuntimeException("test");

        ErrorHandlers.onError(testError);

        assertThat(capturedErrors).containsExactly(testError);
    }

    @Test
    @DisplayName("onError без обработчика печатает в System.err")
    void shouldPrintToSystemErrWhenNoHandler() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(out));
        try {
            RuntimeException testError = new RuntimeException("test");
            ErrorHandlers.onError(testError);

            assertThat(out.toString()).contains("java.lang.RuntimeException: test");
        } finally {
            System.setErr(original);
        }
    }

    @Test
    @DisplayName("setErrorHandler заменяет предыдущий обработчик")
    void shouldReplacePreviousHandler() {
        List<Throwable> first = new ArrayList<>();
        List<Throwable> second = new ArrayList<>();
        ErrorHandlers.setErrorHandler(first::add);
        ErrorHandlers.setErrorHandler(second::add);

        RuntimeException testError = new RuntimeException("test");
        ErrorHandlers.onError(testError);

        assertThat(first).isEmpty();
        assertThat(second).containsExactly(testError);
    }

    @Test
    @DisplayName("reset сбрасывает обработчик")
    void shouldResetHandler() {
        List<Throwable> captured = new ArrayList<>();
        ErrorHandlers.setErrorHandler(captured::add);
        ErrorHandlers.reset();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(out));
        try {
            ErrorHandlers.onError(new RuntimeException("test"));
            assertThat(captured).isEmpty();
            assertThat(out.toString()).isNotEmpty();
        } finally {
            System.setErr(original);
        }
    }

    @Test
    @DisplayName("ошибка внутри обработчика не проглатывается")
    void shouldHandleErrorInsideHandler() {
        ErrorHandlers.setErrorHandler(t -> {
            throw new RuntimeException("handler error");
        });

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(out));
        try {
            ErrorHandlers.onError(new RuntimeException("original"));

            String output = out.toString();
            assertThat(output).contains("handler error");
        } finally {
            System.setErr(original);
        }
    }

    @Test
    @DisplayName("обработчик вызывается один раз за onError")
    void shouldCallHandlerOncePerOnError() {
        AtomicInteger callCount = new AtomicInteger();
        ErrorHandlers.setErrorHandler(t -> callCount.incrementAndGet());

        ErrorHandlers.onError(new RuntimeException("test"));

        assertThat(callCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("null обработчик разрешён (отключает обработчик)")
    void shouldAllowNullHandler() {
        ErrorHandlers.setErrorHandler(t -> {});
        ErrorHandlers.setErrorHandler(null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(out));
        try {
            ErrorHandlers.onError(new RuntimeException("test"));
            assertThat(out.toString()).isNotEmpty();
        } finally {
            System.setErr(original);
        }
    }
}
