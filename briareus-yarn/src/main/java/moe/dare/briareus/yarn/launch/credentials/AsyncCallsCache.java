package moe.dare.briareus.yarn.launch.credentials;

import moe.dare.briareus.common.concurrent.CompletableFutures;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class AsyncCallsCache<T, U> {
    private final ConcurrentMap<T, RefEqualsWrap<U>> cache = new ConcurrentHashMap<>();
    private final RefEqualsWrap<U> dummy = new RefEqualsWrap<>(CompletableFutures
            .failedCompletableFuture(new AssertionError("Helper Completable future must not be queried")));

    AsyncCallsCache() {}

    public CompletableFuture<U> callOrCache(@NotNull T arg, @NotNull Function<T, CompletableFuture<U>> call) {
        requireNonNull(arg);
        requireNonNull(call);
        RefEqualsWrap<U> cachedRef = cache.getOrDefault(arg, dummy);
        if (!cachedRef.future.isDone()) {
            return cachedRef.future;
        }
        cache.remove(arg, cachedRef);
        CompletableFuture<U> newFuture = call.apply(arg);
        if (newFuture.isDone()) {
            return newFuture;
        }
        RefEqualsWrap<U> newRef = new RefEqualsWrap<>(newFuture);
        cache.putIfAbsent(arg, newRef);
        newFuture.whenComplete((anyResult, anyError) -> cache.remove(arg, cachedRef));
        return newFuture;
    }

    private static final class RefEqualsWrap<T> {
        private final CompletableFuture<T> future;

        private RefEqualsWrap(CompletableFuture<T> future) {
            this.future = future;
        }

        @Override
        public boolean equals(Object o) {
            return o == this || o != null && o.getClass() == RefEqualsWrap.class && future == ((RefEqualsWrap<?>) o).future;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(future);
        }
    }
}
