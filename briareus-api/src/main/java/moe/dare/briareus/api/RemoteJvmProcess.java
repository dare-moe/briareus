package moe.dare.briareus.api;

import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;

public interface RemoteJvmProcess {
    /**
     * Attempts to kill JVM. If the process is not alive, no action is taken.
     */
    void destroy();

    /**
     * Attempts to kill JVM forcibly. If the process is not alive, no action is taken.
     */
    void destroyForcibly();

    /**
     * Tests if JVM process is running.
     *
     * @return {@code true} if JVM represented by this instance is running or information about it's termination isn't
     * available yet.
     */
    boolean isAlive();

    /**
     * Returns finished JVM's exit code if available. If JVM is not terminated yet - returns empty optional.
     * This method may always return empty optional if implementation can't access it.
     *
     * @return finished JVM's exit code if available.
     */
    OptionalInt exitCode();

    /**
     * Similar to Java 9+ ProcessHandle#onExit().
     * Returns a CompletableFuture for the termination of the JVM.
     * When the process has terminated the CompletableFuture is completed regardless of the exit status of the process.
     * <br>
     * Whenever returned future may complete exceptionally or not depends on implementation.
     * <br>
     * Cancelling the CompleteableFuture does not affect the remote JVM
     *
     * @return CompleteionStage
     */
    CompletionStage<RemoteJvmProcess> onExit();

    /**
     * Get the external system specific identifier for this process.
     *
     * @return external id of this process or null if not available.
     */
    Object getExternalId();
}