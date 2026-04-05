package com.dfedorino.rxjava.operators.predicate;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.ObservableEmitter;
import com.dfedorino.rxjava.util.TestObserver;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FilterOperatorTest {

    @Test
    @DisplayName("пропускает только элементы, удовлетворяющие предикату")
    void shouldPassOnlyMatching() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 10; i++) emitter.onNext(i);
                    emitter.onComplete();
                })
                .filter(x -> x % 2 == 0)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        assertThat(received)
                .hasSize(5)
                .containsExactly(2, 4, 6, 8, 10);
    }

    @Test
    @DisplayName("пропускает все элементы при предикате true")
    void shouldPassAllWhenAlwaysTrue() {
        List<Integer> received = new ArrayList<>();
        List<Integer> source = List.of(1, 2, 3, 4, 5);

        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        assertThat(received).isEqualTo(source);
    }

    @Test
    @DisplayName("фильтрует все элементы при предикате false")
    void shouldFilterAllWhenAlwaysFalse() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 5; i++) emitter.onNext(i);
                    emitter.onComplete();
                })
                .filter(x -> false)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("обрабатывает пустой поток")
    void shouldHandleEmptyStream() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(ObservableEmitter::onComplete)
                .filter(x -> x > 5)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("фильтрует null значения")
    void shouldFilterNullValues() {
        List<String> received = new ArrayList<>();

        Observable.<String>create(emitter -> {
                    emitter.onNext("Hello");
                    emitter.onNext(null);
                    emitter.onNext("World");
                    emitter.onComplete();
                })
                .filter(Objects::nonNull)
                .subscribe(TestObserver.<String>builder()
                        .onNextAction(received::add)
                        .build());

        assertThat(received)
                .hasSize(2)
                .containsExactly("Hello", "World");
    }

    @Test
    @DisplayName("цепочка из нескольких filter")
    void shouldChainMultipleFilters() {
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
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .build());

        assertThat(received)
                .hasSize(2)
                .containsExactly(3, 4);
    }

    @Test
    @DisplayName("перебрасывает ошибку от источника в onError")
    void shouldPropagateUpstreamError() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Test error");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(10);
                    emitter.onError(testError);
                })
                .filter(x -> x > 5)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onErrorAction(capturedError::set)
                        .build());

        assertThat(received)
                .hasSize(1)
                .containsExactly(10);
        assertThat(capturedError.get())
                .isSameAs(testError);
    }

    @Test
    @DisplayName("перебрасывает исключение из предиката в onError")
    void shouldPropagatePredicateException() {
        List<Integer> received = new ArrayList<>();
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
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onErrorAction(capturedError::set)
                        .build());

        assertThat(received).hasSize(1).containsExactly(1);
        assertThat(capturedError.get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Exception in predicate");
    }

    @Test
    @DisplayName("останавливает эмиссию после dispose")
    void shouldStopAfterDispose() {
        AtomicInteger emitCount = new AtomicInteger();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .filter(x -> x % 2 == 0)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> emitCount.incrementAndGet())
                        .onSubscribeAction(disposableRef::set)
                        .build());

        assertThat(disposableRef.get()).isNotNull();
        disposableRef.get().dispose();
        assertThat(emitCount).hasValue(5);
    }

    @Test
    @DisplayName("не вызывает предикат после dispose")
    void shouldNotCallPredicateAfterDispose() {
        AtomicInteger predicateCallCount = new AtomicInteger();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        CountDownLatch disposedLatch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        emitter.onNext(i);
                        if (i == 5) {
                            disposableRef.get().dispose();
                            disposedLatch.countDown();
                        }
                    }
                    emitter.onComplete();
                })
                .filter(x -> {
                    int count = predicateCallCount.incrementAndGet();
                    if (disposedLatch.getCount() == 0)
                        Assertions.fail("Predicate called after dispose, call #" + count);
                    return true;
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onSubscribeAction(disposableRef::set)
                        .build());

        assertThat(predicateCallCount.get()).isLessThanOrEqualTo(5);
    }

    @Test
    @DisplayName("обрабатывает один элемент")
    void shouldHandleSingleElement() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(42);
                    emitter.onComplete();
                })
                .filter(x -> x > 10)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(received)
                .hasSize(1)
                .containsExactly(42);
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("обрабатывает один отфильтрованный элемент")
    void shouldHandleSingleFilteredOut() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(5);
                    emitter.onComplete();
                })
                .filter(x -> x > 10)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("вызывает предикат ровно один раз на элемент")
    void shouldCallPredicateExactlyOnce() {
        AtomicInteger predicateCallCount = new AtomicInteger();
        List<Integer> source = List.of(1, 2, 3, 4, 5);

        Observable.<Integer>create(emitter -> {
                    source.forEach(emitter::onNext);
                    emitter.onComplete();
                }).filter(x -> {
                    predicateCallCount.incrementAndGet();
                    return x % 2 == 0;
                })
                .subscribe(TestObserver.<Integer>builder().build());

        assertThat(predicateCallCount).hasValue(5);
    }

    @Test
    @DisplayName("пробрасывает ошибку до любого onNext")
    void shouldPropagateErrorBeforeOnNext() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        AtomicBoolean receivedOnNext = new AtomicBoolean();
        RuntimeException testError = new RuntimeException("Immediate error");

        Observable.<Integer>create(emitter -> emitter.onError(testError))
                .filter(x -> x > 0)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> receivedOnNext.set(true))
                        .onErrorAction(capturedError::set)
                        .build());

        assertThat(receivedOnNext).isFalse();
        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("выбрасывает NPE при null предикате")
    void shouldThrowNPEForNullPredicate() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .filter(null)
                .subscribe(TestObserver.<Integer>builder().onErrorAction(capturedError::set).build());

        assertThat(capturedError.get()).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("игнорирует повторный onComplete")
    void shouldIgnoreSecondOnComplete() {
        AtomicInteger completeCount = new AtomicInteger();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                    emitter.onComplete();
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                        })
                        .onCompleteAction(completeCount::incrementAndGet)
                        .build());

        assertThat(completeCount).hasValue(1);
    }

    @Test
    @DisplayName("игнорирует onError после onComplete")
    void shouldIgnoreOnErrorAfterOnComplete() {
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                    emitter.onError(new RuntimeException("Ignored"));
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {
                        })
                        .onCompleteAction(() -> completed.set(true))
                        .onErrorAction(capturedError::set)
                        .build());

        assertThat(completed).isTrue();
        assertThat(capturedError.get()).isNull();
    }

    @Test
    @DisplayName("игнорирует onComplete после onError")
    void shouldIgnoreOnCompleteAfterOnError() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean();
        RuntimeException testError = new RuntimeException("Test error");

        Observable.<Integer>create(emitter -> {
                    emitter.onError(testError);
                    emitter.onComplete();
                })
                .filter(x -> true)
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(capturedError::set)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(capturedError.get()).isSameAs(testError);
        assertThat(completed).isFalse();
    }

    @Test
    @DisplayName("не испускает элементы после ошибки предиката")
    void shouldNotEmitAfterPredicateError() {
        AtomicInteger predicateCallCount = new AtomicInteger();
        AtomicInteger emitCount = new AtomicInteger();

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .filter(x -> {
                    predicateCallCount.incrementAndGet();
                    if (x == 3) throw new IllegalStateException("Error at 3");
                    return true;
                })
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> emitCount.incrementAndGet())
                        .build());

        assertThat(emitCount).hasValue(2);
        assertThat(predicateCallCount.get()).isEqualTo(3);
    }
}
