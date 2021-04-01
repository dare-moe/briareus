package moe.dare.briareus.api;

import java.util.concurrent.CompletionStage;

/**
 * Context that is used to start JVMs.
 *
 * @param <T> Jvm process type
 */
public interface BriareusContext<T extends RemoteJvmProcess> extends AutoCloseable {
    /**
     * @param options options describing JVM instance
     * @return completion stage for started jvm
     */
    CompletionStage<T> start(RemoteJvmOptions options);
}
