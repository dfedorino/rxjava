package com.dfedorino.rxjava.core;

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
        List<Integer> received = new ArrayList<>();

        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .subscribe(new SimpleObserver<>(received::add));

        assertEquals(3, received.size());
        assertEquals(List.of(1, 2, 3), received);
    }

    @Test
    @DisplayName("Observable передаёт ошибку через onError")
    void testObservableEmitsError() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean errorReceived = new AtomicBoolean(false);
        Throwable[] capturedError = new Throwable[1];
        RuntimeException testError = new RuntimeException("Test error");

        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onError(testError);
                })
                .subscribe(new SimpleObserver<>(
                        received::add,
                        t -> {
                            errorReceived.set(true);
                            capturedError[0] = t;
                        },
                        null
                ));

        assertEquals(1, received.size());
        assertEquals(1, received.getFirst());
        assertTrue(errorReceived.get());
        assertEquals(testError, capturedError[0]);
    }

    @Test
    @DisplayName("Observable передаёт уведомление о завершении")
    void testObservableEmitsComplete() {
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribe(new SimpleObserver<>(null, null, () -> completed.set(true)));

        assertTrue(completed.get());
    }

    @Test
    @DisplayName("Observable прекращает эмиссию после dispose")
    void testObservableStopsAfterDispose() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitterRef.set(emitter);
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                })
                .subscribe(new SimpleObserver<>(
                        item -> {
                            received.add(item);
                            if (item == 2) {
                                emitterRef.get().dispose();
                            }
                        },
                        null, null
                ));

        assertEquals(2, received.size());
        assertEquals(List.of(1, 2), received);
    }

    @Test
    @DisplayName("Observable обрабатывает исключение в subscribe")
    void testObservableHandlesExceptionInSubscribe() {
        AtomicBoolean errorReceived = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> {
                    throw new RuntimeException("Exception in subscribe");
                })
                .subscribe(new SimpleObserver<>(
                        null,
                        t -> {
                            System.out.println(">> Exception: " + t.getMessage());
                            errorReceived.set(true);
                        },
                        null
                ));

        assertTrue(errorReceived.get());
    }

    @Test
    @DisplayName("onError прекращает дальнейшую эмиссию")
    void testOnErrorStopsEmission() {
        List<String> received = new ArrayList<>();

        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onError(new RuntimeException("Test error"));
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .subscribe(new SimpleObserver<>(
                        item -> received.add("onNext:" + item),
                        t -> received.add("onError:" + t.getMessage()),
                        () -> received.add("onComplete")
                ));

        assertEquals(2, received.size());
        assertEquals("onNext:1", received.get(0));
        assertEquals("onError:Test error", received.get(1));
    }

    @Test
    @DisplayName("onComplete прекращает дальнейшую эмиссию")
    void testOnCompleteStopsEmission() {
        List<String> received = new ArrayList<>();

        Observable.create((ObservableOnSubscribe<Integer>) emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                    emitter.onNext(2);
                    emitter.onError(new RuntimeException("Test error"));
                })
                .subscribe(new SimpleObserver<>(
                        item -> received.add("onNext:" + item),
                        t -> received.add("onError:" + t.getMessage()),
                        () -> received.add("onComplete")
                ));

        assertEquals(2, received.size());
        assertEquals("onNext:1", received.get(0));
        assertEquals("onComplete", received.get(1));
    }

    @Test
    @DisplayName("isDisposed возвращает true после dispose")
    void testIsDisposedAfterDispose() {
        AtomicReference<ObservableEmitter<Integer>> emitterRef = new AtomicReference<>();

        Observable.<Integer>create(emitterRef::set)
                .subscribe(new SimpleObserver<>(null, null, null));

        assertFalse(emitterRef.get().isDisposed());
        emitterRef.get().dispose();

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
