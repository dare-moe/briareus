package moe.dare.briareus.common.concurrent;

public interface CancelToken {
    boolean isCancellationRequested();

    default void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new TokenCanceledException();
        }
    }
}
