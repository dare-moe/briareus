package moe.dare.briareus.common.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;

class CancelTokenSourceImpl implements CancelTokenSource {
    private final AtomicBoolean flag = new AtomicBoolean();
    private final TokenImpl token = new TokenImpl(flag);

    @Override
    public CancelToken token() {
        return token;
    }

    @Override
    public void cancel() {
        flag.set(true);
    }

    private static class TokenImpl implements CancelToken {
        private final AtomicBoolean flag;

        private TokenImpl(AtomicBoolean flag) {
            this.flag = flag;
        }

        @Override
        public boolean isCancellationRequested() {
            return flag.get();
        }
    }
}
