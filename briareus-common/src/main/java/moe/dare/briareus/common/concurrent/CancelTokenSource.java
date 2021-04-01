package moe.dare.briareus.common.concurrent;

public interface CancelTokenSource {
    static CancelTokenSource newTokenSource() {
        return new CancelTokenSourceImpl();
    }

    CancelToken token();

    void cancel();
}
