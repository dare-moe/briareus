package moe.dare.briareus.local;

import moe.dare.briareus.api.RemoteJvmProcess;

import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

class LocalJvmProcess implements RemoteJvmProcess {
    private final Process process;
    private final CompletableFuture<?> terminatedFuture;

    LocalJvmProcess(Process process, CompletableFuture<?> terminatedFuture) {
        this.process = requireNonNull(process);
        this.terminatedFuture = requireNonNull(terminatedFuture);
    }

    @Override
    public void destroy() {
        process.destroy();
    }

    @Override
    public void destroyForcibly() {
        process.destroyForcibly();
    }

    @Override
    public boolean isAlive() {
        return process.isAlive();
    }

    @Override
    public OptionalInt exitCode() {
        return process.isAlive()? OptionalInt.empty() : OptionalInt.of(process.exitValue());
    }

    @Override
    public CompletionStage<RemoteJvmProcess> onExit() {
        return terminatedFuture.thenApply(any -> this);
    }
}