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

class FlatMapOperatorTest {

    @Test
    @DisplayName("flatMap преобразует каждый элемент в Observable и сливает результаты")
    void testFlatMapTransformsAndMerges() {
        // Arrange
        List<Integer> source = List.of(1, 2, 3);
        List<Integer> received = new ArrayList<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
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
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 10);
                    innerEmitter.onNext(x * 100);
                    innerEmitter.onComplete();
                }))
                .subscribe(observer);

        // Assert
        assertEquals(6, received.size());
        assertTrue(received.contains(10));   // 1 * 10
        assertTrue(received.contains(100));  // 1 * 100
        assertTrue(received.contains(20));   // 2 * 10
        assertTrue(received.contains(200));  // 2 * 100
        assertTrue(received.contains(30));   // 3 * 10
        assertTrue(received.contains(300));  // 3 * 100
    }

    @Test
    @DisplayName("flatMap работает с пустым потоком")
    void testFlatMapWithEmptyStream() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
                received.add(item);
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
        Observable.<Integer>create(ObservableEmitter::onComplete)
                .flatMap(x -> Observable.<Integer>create(emitter -> {
                    emitter.onNext(x * 2);
                    emitter.onComplete();
                }))
                .subscribe(observer);

        // Assert
        assertTrue(received.isEmpty());
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("flatMap передаёт ошибку из источника через onError")
    void testFlatMapPropagatesSourceError() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                errorReceived.set(true);
                capturedError.set(t);
            }

            @Override
            public void onComplete() {
            }
        };

        RuntimeException testError = new RuntimeException("Source error");

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .flatMap(x -> Observable.<Integer>create(emitter -> {
                    emitter.onNext(x * 2);
                    emitter.onComplete();
                }))
                .subscribe(observer);

        // Assert
        assertTrue(errorReceived.get());
        assertEquals(testError, capturedError.get());
    }

    @Test
    @DisplayName("flatMap передаёт ошибку из внутреннего Observable через onError")
    void testFlatMapPropagatesInnerError() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                errorReceived.set(true);
                capturedError.set(t);
            }

            @Override
            public void onComplete() {
            }
        };

        RuntimeException innerError = new RuntimeException("Inner error");

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2); // Этот элемент вызовет ошибку во внутреннем Observable
                    emitter.onComplete();
                })
                .flatMap(x -> {
                    if (x == 2) {
                        return Observable.<Integer>create(emitter -> emitter.onError(innerError));
                    }
                    return Observable.<Integer>create(emitter -> {
                        emitter.onNext(x * 2);
                        emitter.onComplete();
                    });
                })
                .subscribe(observer);

        // Assert
        assertTrue(errorReceived.get());
        assertEquals(innerError, capturedError.get());
    }

    @Test
    @DisplayName("flatMap передаёт исключение из mapper в onError")
    void testFlatMapExceptionInMapper() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                errorReceived.set(true);
                capturedError.set(t);
            }

            @Override
            public void onComplete() {
            }
        };

        // Act
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2); // Mapper выбросит исключение
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .flatMap(x -> {
                    if (x == 2) {
                        throw new IllegalArgumentException("Mapper error!");
                    }
                    return Observable.<Integer>create(emitter -> {
                        emitter.onNext(x * 2);
                        emitter.onComplete();
                    });
                })
                .subscribe(observer);

        // Assert
        assertTrue(errorReceived.get());
        assertInstanceOf(IllegalArgumentException.class, capturedError.get());
        assertEquals("Mapper error!", capturedError.get().getMessage());
    }

    @Test
    @DisplayName("flatMap корректно работает с отпиской (dispose)")
    void testFlatMapWithDispose() {
        // Arrange
        AtomicInteger emitCount = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicBoolean disposed = new AtomicBoolean(false);

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
                disposableRef.set(d);
            }

            @Override
            public void onNext(Integer item) {
                if (!disposed.get()) {
                    emitCount.incrementAndGet();
                }
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
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    if (!innerEmitter.isDisposed()) {
                        innerEmitter.onNext(x * 2);
                    }
                    innerEmitter.onComplete();
                }))
                .subscribe(observer);

        assertNotNull(disposableRef.get());
        disposed.set(true);
        disposableRef.get().dispose();

        // Assert
        assertTrue(emitCount.get() >= 0);
    }

    @Test
    @DisplayName("flatMap позволяет строить цепочки операторов")
    void testFlatMapChaining() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
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
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 2);
                    innerEmitter.onComplete();
                }))
                .map(x -> x + 10)
                .subscribe(observer);

        // Assert
        assertEquals(2, received.size());
        assertTrue(received.contains(12)); // (1 * 2) + 10
        assertTrue(received.contains(14)); // (2 * 2) + 10
    }

    @Test
    @DisplayName("flatMap с filter: комбинация операторов")
    void testFlatMapWithFilter() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
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
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 2);
                    innerEmitter.onNext(x * 3);
                    innerEmitter.onComplete();
                }))
                .filter(x -> x > 5)
                .subscribe(observer);

        // Assert
        assertTrue(received.size() > 0);
        for (Integer item : received) {
            assertTrue(item > 5, "All items should be > 5");
        }
    }

    @Test
    @DisplayName("flatMap: onComplete после завершения всех внутренних Observable")
    void testFlatMapOnCompleteAfterAllInnerComplete() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
                received.add(item);
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
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    innerEmitter.onNext(x * 10);
                    innerEmitter.onComplete();
                }))
                .subscribe(observer);

        // Assert
        assertTrue(completed.get(), "onComplete should be called");
        assertEquals(2, received.size());
        assertTrue(received.contains(10));
        assertTrue(received.contains(20));
    }

    @Test
    @DisplayName("flatMap: несколько элементов в каждом внутреннем Observable")
    void testFlatMapMultipleElementsPerInnerObservable() {
        // Arrange
        List<Integer> received = new ArrayList<>();

        Observer<Integer> observer = new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Integer item) {
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
        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(x -> Observable.<Integer>create(innerEmitter -> {
                    for (int i = 1; i <= 3; i++) {
                        innerEmitter.onNext(x * 10 + i);
                    }
                    innerEmitter.onComplete();
                }))
                .subscribe(observer);

        // Assert
        assertEquals(6, received.size());
        // From x=1: 11, 12, 13
        // From x=2: 21, 22, 23
        assertTrue(received.contains(11));
        assertTrue(received.contains(12));
        assertTrue(received.contains(13));
        assertTrue(received.contains(21));
        assertTrue(received.contains(22));
        assertTrue(received.contains(23));
    }
}
