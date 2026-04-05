package com.dfedorino.rxjava.util;

import com.dfedorino.rxjava.core.Disposable;

public class NoOpDisposable implements Disposable {
    @Override
    public void dispose() {
    }

    @Override
    public boolean isDisposed() {
        return false;
    }
}
