package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.disposable.Disposable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class ObserverTest {
    
    @Test
    @DisplayName("Observer получает элементы через onNext")
    void testOnNext() {
        // Arrange
        List<String> received = new ArrayList<>();
        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
                received.add(item);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        };
        
        // Act
        observer.onNext("Hello");
        observer.onNext("World");
        
        // Assert
        assertEquals(2, received.size());
        assertEquals("Hello", received.get(0));
        assertEquals("World", received.get(1));
    }
    
    @Test
    @DisplayName("Observer получает ошибку через onError")
    void testOnError() {
        // Arrange
        List<Throwable> received = new ArrayList<>();;

        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
            }

            @Override
            public void onError(Throwable t) {
                received.add(t);
            }

            @Override
            public void onComplete() {
            }
        };
        
        // Act
        RuntimeException testError = new RuntimeException("Test error");
        observer.onError(testError);
        
        // Assert
        assertEquals(testError, received.getFirst());
    }
    
    @Test
    @DisplayName("Observer получает уведомление о завершении через onComplete")
    void testOnComplete() {
        // Arrange
        AtomicBoolean completed = new AtomicBoolean(false);

        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        };
        
        // Act
        observer.onComplete();
        
        // Assert
        assertTrue(completed.get());
    }
    
    @Test
    @DisplayName("Observer корректно работает с null значениями")
    void testOnNextWithNull() {
        // Arrange
        Object[] received = new Object[1];
        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
                received[0] = item;
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        };
        
        // Act & Assert
        assertDoesNotThrow(() -> observer.onNext(null));
        assertNull(received[0]);
    }
}
