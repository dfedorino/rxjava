package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.disposable.Disposable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ObservableTest {

    @Test
    @DisplayName("Observable передаёт элементы через onNext")
    void testObservableEmitsItems() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        Observer<Integer> observer = new SimpleObserver<>(received::add);

        Observable<Integer> observable = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        });

        // Act
        observable.subscribe(observer);

        // Assert
        assertEquals(3, received.size());
        assertEquals(List.of(1, 2, 3), received);
    }

    @Test
    @DisplayName("Observable передаёт ошибку через onError")
    void testObservableEmitsError() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        Throwable[] capturedError = new Throwable[1];

        Observer<Integer> observer = new SimpleObserver<>(
                received::add,
                t -> {
                    errorReceived.set(true);
                    capturedError[0] = t;
                },
                null
        );

        RuntimeException testError = new RuntimeException("Test error");

        Observable<Integer> observable = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onError(testError);
        });

        // Act
        observable.subscribe(observer);

        // Assert
        assertEquals(1, received.size());
        assertEquals(1, received.getFirst());
        assertTrue(errorReceived.get());
        assertEquals(testError, capturedError[0]);
    }

    @Test
    @DisplayName("Observable передаёт уведомление о завершении")
    void testObservableEmitsComplete() {
        // Arrange
        AtomicBoolean completed = new AtomicBoolean(false);

        Observer<Integer> observer = new SimpleObserver<>(
                null,
                null,
                () -> completed.set(true)
        );

        Observable<Integer> observable = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        });

        // Act
        observable.subscribe(observer);

        // Assert
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("Observable прекращает эмиссию после dispose")
    void testObservableStopsAfterDispose() {
        // Arrange
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observer<Integer> observer = new SimpleObserver<>(
                item -> {
                    received.add(item);
                    if (item == 2) {
                        emitterRef.get().dispose();
                    }
                },
                null,
                null
        );

        Observable<Integer> observable = Observable.create(emitter -> {
            emitterRef.set(emitter);
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3); // Не должно быть получено
            emitter.onNext(4); // Не должно быть получено
        });

        // Act
        observable.subscribe(observer);

        // Assert
        assertEquals(2, received.size());
        assertEquals(List.of(1, 2), received);
    }

    @Test
    @DisplayName("Observable обрабатывает исключение в subscribe")
    void testObservableHandlesExceptionInSubscribe() {
        // Arrange
        AtomicBoolean errorReceived = new AtomicBoolean(false);

        Observer<Integer> observer = new SimpleObserver<>(
                null,
                t -> {
                    System.out.println(">> Exception: " + t.getMessage());
                    errorReceived.set(true);
                },
                null
        );

        Observable<Integer> observable = Observable.create(emitter -> {
            throw new RuntimeException("Exception in subscribe");
        });

        // Act
        observable.subscribe(observer);

        // Assert
        assertTrue(errorReceived.get());
    }

    @Test
    @DisplayName("onError прекращает дальнейшую эмиссию")
    void testOnErrorStopsEmission() {
        // Arrange
        List<String> received = new ArrayList<>();

        Observer<Integer> observer = new SimpleObserver<>(
                item -> received.add("onNext:" + item),
                t -> received.add("onError:" + t.getMessage()),
                () -> received.add("onComplete")
        );

        Observable<Integer> observable = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onError(new RuntimeException("Test error"));
            emitter.onNext(2); // Не должно быть получено
            emitter.onComplete(); // Не должно быть получено
        });

        // Act
        observable.subscribe(observer);

        // Assert
        assertEquals(2, received.size());
        assertEquals("onNext:1", received.get(0));
        assertEquals("onError:Test error", received.get(1));
    }

    @Test
    @DisplayName("onComplete прекращает дальнейшую эмиссию")
    void testOnCompleteStopsEmission() {
        // Arrange
        List<String> received = new ArrayList<>();

        Observer<Integer> observer = new SimpleObserver<>(
                item -> received.add("onNext:" + item),
                t -> received.add("onError:" + t.getMessage()),
                () -> received.add("onComplete")
        );

        Observable<Integer> observable = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            emitter.onNext(2); // Не должно быть получено
            emitter.onError(new RuntimeException("Test error")); // Не должно быть получено
        });

        // Act
        observable.subscribe(observer);

        // Assert
        assertEquals(2, received.size());
        assertEquals("onNext:1", received.get(0));
        assertEquals("onComplete", received.get(1));
    }

    @Test
    @DisplayName("isDisposed возвращает true после dispose")
    void testIsDisposedAfterDispose() {
        // Arrange
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observer<Integer> observer = new SimpleObserver<>(
                null,
                null,
                null
        );

        Observable<Integer> observable = Observable.create(emitterRef::set);

        // Act
        observable.subscribe(observer);
        assertFalse(emitterRef.get().isDisposed());

        emitterRef.get().dispose();

        // Assert
        assertTrue(emitterRef.get().isDisposed());
    }

    // Вспомогательный класс для тестов
    private static class SimpleObserver<T> implements Observer<T> {
        private final Consumer<T> onNextAction;
        private final Consumer<Throwable> onErrorAction;
        private final Runnable onCompleteAction;

        SimpleObserver(Consumer<T> onNextAction) {
            this(onNextAction,
                    t -> System.err.println(">> exception: " + t.getMessage()),
                    () -> System.out.println(">> onComplete is called")
            );
        }

        SimpleObserver(Consumer<T> onNextAction, Consumer<Throwable> onErrorAction, Runnable onCompleteAction) {
            this.onNextAction = onNextAction;
            this.onErrorAction = onErrorAction != null ? onErrorAction : t -> {};
            this.onCompleteAction = onCompleteAction != null ? onCompleteAction : () -> {};
        }

        @Override
        public void onSubscribe(Disposable d) {
            System.out.println(">> onSubscribe");
        }

        @Override
        public void onNext(T item) {
            if (onNextAction != null) onNextAction.accept(item);
        }

        @Override
        public void onError(Throwable t) {
            onErrorAction.accept(t);
        }

        @Override
        public void onComplete() {
            onCompleteAction.run();
        }
    }
}
