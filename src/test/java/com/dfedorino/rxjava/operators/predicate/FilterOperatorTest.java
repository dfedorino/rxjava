package com.dfedorino.rxjava.operators.predicate;

import com.dfedorino.rxjava.core.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FilterOperatorTest {

    @Test
    @DisplayName("filter пропускает только элементы, удовлетворяющие предикату")
    void testFilterPassesMatchingElements() {
        List<Integer> source = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> x % 2 == 0)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(Integer item) { received.add(item); }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() {}
                });

        assertEquals(5, received.size());
        assertEquals(List.of(2, 4, 6, 8, 10), received);
    }

    @Test
    @DisplayName("filter работает с пустым потоком")
    void testFilterWithEmptyStream() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(ObservableEmitter::onComplete)
                .filter(x -> x > 5)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(Integer item) { received.add(item); }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() { completed.set(true); }
                });

        assertTrue(received.isEmpty());
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("filter передаёт ошибку через onError")
    void testFilterPropagatesError() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(10);
                    emitter.onError(testError);
                })
                .filter(x -> x > 5)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(Integer item) { received.add(item); }
                    @Override public void onError(Throwable t) {
                        errorReceived.set(true);
                        capturedError.set(t);
                    }
                    @Override public void onComplete() {}
                });

        assertEquals(1, received.size());
        assertEquals(10, received.getFirst());
        assertTrue(errorReceived.get());
        assertEquals(testError, capturedError.get());
    }

    @Test
    @DisplayName("filter передаёт исключение из predicate в onError")
    void testFilterExceptionInPredicate() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                })
                .filter(x -> {
                    if (x == 2) {
                        throw new IllegalArgumentException("Exception in predicate");
                    }
                    return x > 0;
                })
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(Integer item) { received.add(item); }
                    @Override public void onError(Throwable t) {
                        errorReceived.set(true);
                        capturedError.set(t);
                    }
                    @Override public void onComplete() {}
                });

        assertEquals(1, received.size());
        assertEquals(1, received.getFirst());
        assertTrue(errorReceived.get());
        assertInstanceOf(IllegalArgumentException.class, capturedError.get());
        assertEquals("Exception in predicate", capturedError.get().getMessage());
    }

    @Test
    @DisplayName("filter корректно работает с отпиской (dispose)")
    void testFilterWithDispose() {
        AtomicInteger emitCount = new AtomicInteger(0);
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        if (emitter.isDisposed()) return;
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .filter(x -> x % 2 == 0)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) { disposableRef.set(d); }
                    @Override public void onNext(Integer item) { emitCount.incrementAndGet(); }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() {}
                });

        assertNotNull(disposableRef.get());
        disposableRef.get().dispose();

        assertEquals(5, emitCount.get());
    }

    @Test
    @DisplayName("filter позволяет строить цепочки операторов")
    void testFilterChaining() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onNext(5);
                    emitter.onComplete();
                })
                .filter(x -> x > 2)
                .filter(x -> x < 5)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(Integer item) { received.add(item); }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() {}
                });

        assertEquals(2, received.size());
        assertEquals(List.of(3, 4), received);
    }

    @Test
    @DisplayName("filter пропускает все элементы если предикат всегда true")
    void testFilterWithAlwaysTruePredicate() {
        List<Integer> source = List.of(1, 2, 3, 4, 5);
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> true)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(Integer item) { received.add(item); }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() {}
                });

        assertEquals(5, received.size());
        assertEquals(source, received);
    }

    @Test
    @DisplayName("filter отфильтровывает все элементы если предикат всегда false")
    void testFilterWithAlwaysFalsePredicate() {
        List<Integer> source = List.of(1, 2, 3, 4, 5);
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> false)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(Integer item) { received.add(item); }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() { completed.set(true); }
                });

        assertTrue(received.isEmpty());
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("filter работает с null значениями")
    void testFilterWithNullValues() {
        List<String> received = new ArrayList<>();

        Observable.<String>create(emitter -> {
                    emitter.onNext("Hello");
                    emitter.onNext(null);
                    emitter.onNext("World");
                    emitter.onComplete();
                })
                .filter(Objects::nonNull)
                .subscribe(new Observer<>() {
                    @Override public void onSubscribe(Disposable d) {}
                    @Override public void onNext(String item) { received.add(item); }
                    @Override public void onError(Throwable t) {}
                    @Override public void onComplete() {}
                });

        assertEquals(2, received.size());
        assertEquals("Hello", received.get(0));
        assertEquals("World", received.get(1));
    }
}
