package com.dfedorino.rxjava.core;

import com.dfedorino.rxjava.util.NoOpDisposable;
import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ObserverTest {

    @Test
    @DisplayName("получает элементы через onNext")
    void shouldReceiveItemsViaOnNext() {
        List<String> received = new ArrayList<>();
        Observer<String> observer = TestObserver.<String>builder()
                .onNextAction(received::add)
                .build();

        observer.onNext("Hello");
        observer.onNext("World");

        assertThat(received)
                .hasSize(2)
                .containsExactly("Hello", "World");
    }

    @Test
    @DisplayName("получает ошибку через onError")
    void shouldReceiveErrorViaOnError() {
        AtomicReference<Throwable> error = new AtomicReference<>();
        Observer<String> observer = TestObserver.<String>builder()
                .onErrorAction(error::set)
                .build();
        RuntimeException testError = new RuntimeException("Test error");

        observer.onError(testError);

        assertThat(error.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("получает уведомление о завершении через onComplete")
    void shouldReceiveCompletionViaOnComplete() {
        AtomicBoolean completed = new AtomicBoolean();
        Observer<String> observer = TestObserver.<String>builder()
                .onCompleteAction(() -> completed.set(true))
                .build();

        observer.onComplete();

        assertThat(completed.get()).isTrue();
    }

    @Test
    @DisplayName("обрабатывает последовательность обратных вызовов")
    void shouldHandleCallbackSequence() {
        List<String> events = new ArrayList<>();
        Observer<String> observer = TestObserver.<String>builder()
                .onNextAction(item -> events.add("onNext:" + item))
                .onCompleteAction(() -> events.add("onComplete"))
                .onErrorAction(t -> events.add("onError:" + t.getMessage()))
                .onSubscribeAction(d -> events.add("onSubscribe"))
                .build();

        observer.onSubscribe(new NoOpDisposable());
        observer.onNext("item1");
        observer.onNext("item2");
        observer.onComplete();

        assertThat(events)
                .hasSize(4)
                .containsExactly("onSubscribe", "onNext:item1", "onNext:item2", "onComplete");
    }
}
