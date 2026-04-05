package com.dfedorino.rxjava.operators.transform;

import com.dfedorino.rxjava.core.Disposable;
import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.core.ObservableEmitter;
import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FlatMapOperatorTest {

    @Test
    @DisplayName("преобразует элемент в Observable и объединяет результаты")
    void shouldTransformAndMerge() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onNext(3); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 10); e.onComplete(); }))
                .subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(3).containsExactlyInAnyOrder(10, 20, 30);
    }

    @Test
    @DisplayName("обрабатывает пустой поток")
    void shouldHandleEmptySource() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(ObservableEmitter::onComplete)
                .flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 2); e.onComplete(); }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(received).isEmpty();
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("пробрасывает ошибку от источника")
    void shouldPropagateSourceError() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Source error");

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onError(testError);
        }).flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 2); e.onComplete(); }))
                .subscribe(TestObserver.<Integer>builder().onErrorAction(capturedError::set).build());

        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("пробрасывает ошибку внутреннего Observable")
    void shouldPropagateInnerError() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException innerError = new RuntimeException("Inner error");

        Observable.<Integer>create(emitter -> { emitter.onNext(1); emitter.onComplete(); })
                .flatMap(x -> Observable.<Integer>create(e -> e.onError(innerError)))
                .subscribe(TestObserver.<Integer>builder().onErrorAction(capturedError::set).build());

        assertThat(capturedError.get()).isSameAs(innerError);
    }

    @Test
    @DisplayName("пропагирует исключение mapper")
    void shouldPropagateMapperException() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onNext(3); emitter.onComplete();
        }).flatMap(x -> {
            if (x == 2) throw new IllegalArgumentException("Mapper error!");
            return Observable.<Integer>create(e -> { e.onNext(x * 2); e.onComplete(); });
        }).subscribe(TestObserver.<Integer>builder().onErrorAction(capturedError::set).build());

        assertThat(capturedError.get())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Mapper error!");
    }

    @Test
    @DisplayName("корректно обрабатывает dispose")
    void shouldHandleDispose() {
        AtomicInteger emitCount = new AtomicInteger();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();
        AtomicBoolean disposed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> {
            if (!e.isDisposed()) e.onNext(x * 2);
            e.onComplete();
        })).subscribe(TestObserver.<Integer>builder()
                .onNextAction(item -> { if (!disposed.get()) emitCount.incrementAndGet(); })
                .onSubscribeAction(disposableRef::set)
                .build());

        assertThat(disposableRef.get()).isNotNull();
        disposed.set(true);
        disposableRef.get().dispose();
        assertThat(emitCount.get()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("цепочка с map")
    void shouldChainWithMap() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 2); e.onComplete(); }))
                .map(x -> x + 10)
                .subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(2).containsExactlyInAnyOrder(12, 14);
    }

    @Test
    @DisplayName("цепочка с filter")
    void shouldChainWithFilter() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onNext(3); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 2); e.onNext(x * 3); e.onComplete(); }))
                .filter(x -> x > 5)
                .subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).allMatch(item -> item > 5);
    }

    @Test
    @DisplayName("вызывает onComplete после завершения всех внутренних Observable")
    void shouldCompleteAfterAllInners() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 10); e.onComplete(); }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onCompleteAction(() -> completed.set(true))
                        .build());

        assertThat(completed).isTrue();
        assertThat(received).hasSize(2).containsExactlyInAnyOrder(10, 20);
    }

    @Test
    @DisplayName("испускает несколько элементов из каждого внутреннего Observable")
    void shouldEmitMultipleFromEachInner() {
        List<Integer> received = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> {
            for (int i = 1; i <= 3; i++) e.onNext(x * 10 + i);
            e.onComplete();
        })).subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(6).containsExactlyInAnyOrder(11, 12, 13, 21, 22, 23);
    }

    @Test
    @DisplayName("обрабатывает пустой внутренний Observable")
    void shouldHandleEmptyInner() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onNext(3); emitter.onComplete();
        }).flatMap(x -> {
            if (x == 2) return Observable.<Integer>create(ObservableEmitter::onComplete);
            return Observable.<Integer>create(e -> { e.onNext(x * 10); e.onComplete(); });
        }).subscribe(TestObserver.<Integer>builder()
                .onNextAction(received::add)
                .onCompleteAction(() -> completed.set(true))
                .build());

        assertThat(received).hasSize(2).containsExactlyInAnyOrder(10, 30);
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("один элемент источника в несколько внутренних")
    void shouldHandleSingleToMultiple() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(5); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> {
            e.onNext(x); e.onNext(x + 1); e.onNext(x + 2); e.onComplete();
        })).subscribe(TestObserver.<Integer>builder()
                .onNextAction(received::add)
                .onCompleteAction(() -> completed.set(true))
                .build());

        assertThat(received).hasSize(3).containsExactly(5, 6, 7);
        assertThat(completed).isTrue();
    }

    @Test
    @DisplayName("выбрасывает NPE при null mapper")
    void shouldThrowNPEForNullMapper() {
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        Observable<Integer> source = Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onComplete();
        });

        source.<Object>flatMap(null)
                .subscribe(TestObserver.builder().onErrorAction(capturedError::set).build());

        assertThat(capturedError.get()).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("пробрасывает ошибку при ожидающих внутренних Observable")
    void shouldPropagateErrorWithPendingInners() {
        List<Integer> received = new ArrayList<>();
        AtomicReference<Throwable> capturedError = new AtomicReference<>();
        RuntimeException testError = new RuntimeException("Source error");

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onError(testError);
        }).flatMap(x -> Observable.<Integer>create(e -> e.onNext(x * 10)))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(received::add)
                        .onErrorAction(capturedError::set)
                        .build());

        assertThat(capturedError.get()).isSameAs(testError);
    }

    @Test
    @DisplayName("игнорирует повторный onComplete")
    void shouldIgnoreSecondOnComplete() {
        AtomicInteger completeCount = new AtomicInteger();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onComplete(); emitter.onComplete();
        }).flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 2); e.onComplete(); }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {})
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
            emitter.onNext(1); emitter.onComplete();
            emitter.onError(new RuntimeException("Ignored"));
        }).flatMap(x -> Observable.<Integer>create(e -> { e.onNext(x * 2); e.onComplete(); }))
                .subscribe(TestObserver.<Integer>builder()
                        .onNextAction(item -> {})
                        .onCompleteAction(() -> completed.set(true))
                        .onErrorAction(capturedError::set)
                        .build());

        assertThat(completed).isTrue();
        assertThat(capturedError.get()).isNull();
    }

    @Test
    @DisplayName("обрабатывает mapper, возвращающий один Observable")
    void shouldHandleSameObservable() {
        List<Integer> received = new ArrayList<>();
        Observable<Integer> sameInner = Observable.<Integer>create(emitter -> {
            emitter.onNext(42); emitter.onComplete();
        });

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1); emitter.onNext(2); emitter.onComplete();
        }).flatMap((Integer x) -> sameInner)
                .subscribe(TestObserver.<Integer>builder().onNextAction(received::add).build());

        assertThat(received).hasSize(2).containsExactly(42, 42);
    }
}
