package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MapOperatorTest {

    @Test
    @DisplayName("map преобразует каждый элемент потока")
    void testMapTransformsElements() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3, 4, 5);
        List<String> received = new ArrayList<>();

        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
                received.add(item);
                System.out.println(">> added " + item);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
                System.out.println(">> onComplete");
            }
        };

        // Act
        Observable.create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .map(i -> "Value-" + i)
                .subscribe(observer);

        // Assert
        assertEquals(5, received.size());
        assertEquals("Value-1", received.get(0));
        assertEquals("Value-2", received.get(1));
        assertEquals("Value-3", received.get(2));
        assertEquals("Value-4", received.get(3));
        assertEquals("Value-5", received.get(4));
    }

    @Test
    @DisplayName("map работает с пустым потоком")
    void testMapWithEmptyStream() {
        // Arrange
        List<String> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
                received.add(item);
                System.out.println(">> added " + item);
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
        Observable.create(ObservableEmitter::onComplete)
                .map(i -> "Value-" + i)
                .subscribe(observer);

        // Assert
        assertTrue(received.isEmpty());
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("map передаёт ошибку через onError")
    void testMapPropagatesError() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
            }

            @Override
            public void onError(Throwable t) {
                System.out.println(">> captured error " + t.getMessage());
                errorReceived.set(true);
                capturedError.set(t);
            }

            @Override
            public void onComplete() {
                System.out.println(">> onComplete");
            }
        };

        RuntimeException testError = new RuntimeException("Test error");

        // Act
        Observable.create(emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .map(i -> "Value-" + i)
                .subscribe(observer);

        // Assert
        assertTrue(errorReceived.get());
        assertEquals(testError, capturedError.get());
    }

    @Test
    @DisplayName("map передаёт исключение из mapper в onError")
    void testMapExceptionInMapper() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
            }

            @Override
            public void onError(Throwable t) {
                System.out.println(">> captured error " + t.getMessage());
                errorReceived.set(true);
                capturedError.set(t);
            }

            @Override
            public void onComplete() {
                System.out.println(">> onComplete");
            }
        };

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2); // Это вызовет исключение
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .map(i -> {
                    if (i == 2) {
                        throw new IllegalArgumentException("Hate evens!");
                    }
                    return "Value-" + i;
                })
                .subscribe(observer);

        // Assert
        assertTrue(errorReceived.get());
        assertInstanceOf(IllegalArgumentException.class, capturedError.get());
        assertEquals("Hate evens!", capturedError.get().getMessage());
    }

    @Test
    @DisplayName("map корректно работает с отпиской (dispose)")
    void testMapWithDispose() {
        // Arrange
        AtomicInteger emitCount = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
                disposableRef.set(d);
            }

            @Override
            public void onNext(Integer item) {
                emitCount.incrementAndGet();
                System.out.println(">> emitCount " + emitCount.intValue());
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
                System.out.println(">> onComplete");
            }
        };

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        if (emitter.isDisposed()) {
                            return;
                        }
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .map(i -> i * 2)
                .subscribe(observer);

        assertNotNull(disposableRef.get());
        disposableRef.get().dispose();

        // Assert
        assertEquals(10, emitCount.get());
    }

    @Test
    @DisplayName("map позволяет строить цепочки операторов")
    void testMapChaining() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
                received.add(item);
                System.out.println(">> added " + item);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        };

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .map(i -> i * 2)
                .map(i -> i + 10)
                .subscribe(observer);

        // Assert
        assertEquals(3, received.size());
        assertEquals(12, received.get(0)); // (1 * 2) + 10
        assertEquals(14, received.get(1)); // (2 * 2) + 10
        assertEquals(16, received.get(2)); // (3 * 2) + 10
    }

    @Test
    @DisplayName("map работает с null значениями (если mapper возвращает null)")
    void testMapWithNullResult() {
        // Arrange
        List<String> received = new ArrayList<>();

        Observer<String> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(String item) {
                received.add(item);
                System.out.println(">> added " + item);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
                System.out.println(">> onComplete");
            }
        };

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1); // map вернет null
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .map(i -> i == 1 ? null : "Value-" + i)
                .subscribe(observer);

        // Assert
        assertEquals(2, received.size());
        assertNull(received.get(0));
        assertEquals("Value-2", received.get(1));
    }
}
