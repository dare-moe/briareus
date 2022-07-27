package moe.dare.briareus.yarn.launch.credentials;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncCallsCacheTest {
    @Test
    void callsAreCachedTillExecuted() {
        // given
        AsyncCallsCache<String, String> cache = new AsyncCallsCache<>();
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();
        CompletableFuture<String> future3 = new CompletableFuture<>();
        // when
        CompletableFuture<String> call1 = cache.callOrCache("arg", any -> future1);
        CompletableFuture<String> call2 = cache.callOrCache("arg", any -> future2);
        // then
        assertThat(call1).isSameAs(call2);
        assertThat(cache.inFlightCalls()).isOne();
        // when
        future1.complete("foo");
        // then
        CompletableFuture<String> call3 = cache.callOrCache("arg", any -> future3);
        assertThat(call3).isSameAs(future3);
    }

    @Test
    void callsAreRemovedFromCacheWhenExecuted() {
        // given
        AsyncCallsCache<String, String> cache = new AsyncCallsCache<>();
        // when
        cache.callOrCache("arg", any -> new CompletableFuture<>()).complete("foo");
        // then
        assertThat(cache.inFlightCalls()).isZero();
    }
}