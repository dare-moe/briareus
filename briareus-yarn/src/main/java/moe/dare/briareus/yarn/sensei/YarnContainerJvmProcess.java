package moe.dare.briareus.yarn.sensei;

import moe.dare.briareus.api.RemoteJvmProcess;
import moe.dare.briareus.common.concurrent.CompletableFutures;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class YarnContainerJvmProcess implements RemoteJvmProcess {
    private final NMClientAsync nmClientAsync;
    private final ContainerId containerId;
    private final NodeId nodeId;
    private final CompletableFuture<Integer> exitCode;

    YarnContainerJvmProcess(NMClientAsync nmClientAsync, ContainerId containerId, NodeId nodeId, CompletableFuture<Integer> exitCode) {
        this.nmClientAsync = nmClientAsync;
        this.containerId = containerId;
        this.nodeId = nodeId;
        this.exitCode = exitCode;
    }

    @Override
    public void destroy() {
        if (isAlive()) {
            nmClientAsync.stopContainerAsync(containerId, nodeId);
        }
    }

    @Override
    public void destroyForcibly() {
        destroy();
    }

    @Override
    public boolean isAlive() {
        return !exitCode.isDone();
    }

    @Override
    public OptionalInt exitCode() {
        return CompletableFutures.getNow(exitCode).map(OptionalInt::of).orElseGet(OptionalInt::empty);
    }

    @Override
    public CompletionStage<RemoteJvmProcess> onExit() {
        return exitCode.thenApply(any -> this);
    }

    @Override
    public Object getExternalId() {
        return containerId;
    }
}
