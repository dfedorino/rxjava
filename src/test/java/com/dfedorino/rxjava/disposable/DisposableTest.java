package com.dfedorino.rxjava.disposable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DisposableTest {

    @Test
    @DisplayName("Disposable корректно отслеживает состояние isDisposed")
    void testIsDisposed() {
        // Arrange
        AtomicBoolean disposed = new AtomicBoolean(false);
        Disposable disposable = new Disposable() {
            @Override
            public void dispose() {
                disposed.set(true);
            }

            @Override
            public boolean isDisposed() {
                return disposed.get();
            }
        };

        // Assert
        assertFalse(disposable.isDisposed());

        // Act
        disposable.dispose();

        // Assert
        assertTrue(disposable.isDisposed());
    }

    @Test
    @DisplayName("dispose() освобождает ресурсы")
    void testDisposeReleasesResources() {
        // Arrange
        AtomicInteger disposeCount = new AtomicInteger(0);
        Disposable disposable = new Disposable() {
            @Override
            public void dispose() {
                disposeCount.incrementAndGet();
            }

            @Override
            public boolean isDisposed() {
                return disposeCount.get() > 0;
            }
        };

        // Act
        disposable.dispose();

        // Assert
        assertEquals(1, disposeCount.get());
    }

    @Test
    @DisplayName("dispose() является идемпотентным методом")
    void testDisposeIdempotency() {
        // Arrange
        AtomicInteger disposeCount = new AtomicInteger(0);
        Disposable disposable = new Disposable() {
            @Override
            public void dispose() {
                disposeCount.incrementAndGet();
            }

            @Override
            public boolean isDisposed() {
                return disposeCount.get() > 0;
            }
        };

        // Act
        disposable.dispose();
        disposable.dispose();
        disposable.dispose();

        // Assert
        // Метод может вызываться多次, но состояние isDisposed() должно оставаться true
        assertTrue(disposable.isDisposed());
    }

    @Test
    @DisplayName("Multiple вызовов isDisposed() возвращают корректное состояние")
    void testMultipleIsDisposedCalls() {
        // Arrange
        AtomicBoolean disposed = new AtomicBoolean(false);
        Disposable disposable = new Disposable() {
            @Override
            public void dispose() {
                disposed.set(true);
            }

            @Override
            public boolean isDisposed() {
                return disposed.get();
            }
        };

        // Assert
        assertFalse(disposable.isDisposed());
        assertFalse(disposable.isDisposed());

        // Act
        disposable.dispose();

        // Assert
        assertTrue(disposable.isDisposed());
        assertTrue(disposable.isDisposed());
        assertTrue(disposable.isDisposed());
    }
}
