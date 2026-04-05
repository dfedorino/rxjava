package com.dfedorino.rxjava.scheduler;

import com.dfedorino.rxjava.core.Observable;
import com.dfedorino.rxjava.util.TestObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RejectedExecutionTest {

    @Test
    @DisplayName("RejectedExecutionException в subscribeOn → onError")
    void rejectedExecutionInSubscribeOn() throws InterruptedException {
        // Create a fresh scheduler and shut it down
        var scheduler = new IOThreadScheduler();
        scheduler.shutdown();

        List<Throwable> errors = new ArrayList<>();
        CountDownLatch errorLatch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribeOn(scheduler)
                .subscribe(TestObserver.<Integer>builder()
                        .onErrorAction(t -> {
                            errors.add(t);
                            errorLatch.countDown();
                        })
                        .build());

        assertThat(errorLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(errors.getFirst()).isInstanceOf(RejectedExecutionException.class);
    }
}
