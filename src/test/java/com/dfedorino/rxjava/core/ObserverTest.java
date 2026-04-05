package com.dfedorino.rxjava.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Observer")
class ObserverTest {

    @Test
    @DisplayName("should receive items via onNext")
    void shouldReceiveItemsViaOnNext() {
        // Arrange
        List<String> received = new ArrayList<>();
        Observer<String> observer = createTestObserver(
                received::add,
                null,
                null,
                null
        );

        // Act
        observer.onNext("Hello");
        observer.onNext("World");

        // Assert
        assertThat(received)
                .hasSize(2)
                .containsExactly("Hello", "World");
    }

    @Test
    @DisplayName("should receive error via onError")
    void shouldReceiveErrorViaOnError() {
        // Arrange
        AtomicReference<Throwable> receivedError = new AtomicReference<>();
        Observer<String> observer = createTestObserver(
                null,
                receivedError::set,
                null,
                null
        );
        RuntimeException testError = new RuntimeException("Test error");

        // Act
        observer.onError(testError);

        // Assert
        assertThat(receivedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("should receive completion notification via onComplete")
    void shouldReceiveCompletionNotificationViaOnComplete() {
        // Arrange
        AtomicBoolean completed = new AtomicBoolean(false);
        Observer<String> observer = createTestObserver(
                null,
                null,
                () -> completed.set(true),
                null
        );

        // Act
        observer.onComplete();

        // Assert
        assertThat(completed.get()).isTrue();
    }

    @Test
    @DisplayName("should receive Disposable via onSubscribe")
    void shouldReceiveDisposableViaOnSubscribe() {
        // Arrange
        AtomicReference<Disposable> receivedDisposable = new AtomicReference<>();
        Observer<String> observer = createTestObserver(
                null,
                null,
                null,
                receivedDisposable::set
        );
        Disposable testDisposable = new Disposable() {
            @Override
            public void dispose() {}
            @Override
            public boolean isDisposed() { return false; }
        };

        // Act
        observer.onSubscribe(testDisposable);

        // Assert
        assertThat(receivedDisposable.get()).isSameAs(testDisposable);
    }

    @Test
    @DisplayName("should handle multiple callbacks in sequence")
    void shouldHandleMultipleCallbacksInSequence() {
        // Arrange
        List<String> events = new ArrayList<>();
        Observer<String> observer = createTestObserver(
                item -> events.add("onNext:" + item),
                t -> events.add("onError:" + t.getMessage()),
                () -> events.add("onComplete"),
                d -> events.add("onSubscribe")
        );

        // Act
        Disposable testDisposable = new Disposable() {
            @Override
            public void dispose() {}
            @Override
            public boolean isDisposed() { return false; }
        };
        observer.onSubscribe(testDisposable);
        observer.onNext("item1");
        observer.onNext("item2");
        observer.onComplete();

        // Assert
        assertThat(events)
                .hasSize(4)
                .containsExactly("onSubscribe", "onNext:item1", "onNext:item2", "onComplete");
    }

    private static <T> Observer<T> createTestObserver(
            java.util.function.Consumer<T> onNext,
            java.util.function.Consumer<Throwable> onError,
            Runnable onComplete,
            java.util.function.Consumer<Disposable> onSubscribe) {
        return new Observer<>() {
            @Override
            public void onSubscribe(Disposable d) {
                if (onSubscribe != null) onSubscribe.accept(d);
            }

            @Override
            public void onNext(T item) {
                if (onNext != null) onNext.accept(item);
            }

            @Override
            public void onError(Throwable t) {
                if (onError != null) onError.accept(t);
            }

            @Override
            public void onComplete() {
                if (onComplete != null) onComplete.run();
            }
        };
    }
}
