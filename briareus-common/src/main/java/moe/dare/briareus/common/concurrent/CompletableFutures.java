package moe.dare.briareus.common.concurrent;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class CompletableFutures {
    /**
     * Return current result of give future if available or empty future otherwise.
     * Unlike {@link CompletableFuture#getNow(Object)} does not throw exceptions.
     *
     * @param <T> type of future value
     * @param future future to get result from
     * @return optional wrapping future's result o
     */
    public static <T> Optional<T> getNow(CompletableFuture<T> future) {
        if (future.isDone() && !future.isCompletedExceptionally()) {
            return Optional.ofNullable(future.getNow(null));
        }
        return Optional.empty();
    }

    public static <T> CompletableFuture<T> failedCompletableFuture(Throwable cause) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(cause);
        return f;
    }

    private CompletableFutures() {
    }
}
