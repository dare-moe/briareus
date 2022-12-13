package moe.dare.briareus.yarn.sensei;

import moe.dare.briareus.api.JvmStartFailedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

final class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
    private static final Logger log = LoggerFactory.getLogger(NMCallbackHandler.class);
    private final ConcurrentMap<ContainerId, CompletableFuture<Void>> startingFutures;

    NMCallbackHandler(ConcurrentMap<ContainerId, CompletableFuture<Void>> startingFutures) {
        this.startingFutures = requireNonNull(startingFutures);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        log.debug("Container {} started", containerId);
        CompletableFuture<Void> startedFuture = startingFutures.remove(containerId);
        if (startedFuture == null) {
            log.warn("Container {} started but is not tracked for start.", containerId);
            return;
        }
        startedFuture.complete(null);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        log.warn("Container {} failed to start", containerId, t);
        CompletableFuture<Void> startedFuture = startingFutures.remove(containerId);
        if (startedFuture == null) {
            log.error("Container {} failed to start but is not tracked for start.", containerId, t);
            return;
        }
        Exception e = new JvmStartFailedException("Can't start container " + containerId, t);
        startedFuture.completeExceptionally(e);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        log.info("Container {} stopped successfully", containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        log.error("Container {} failed to stop", containerId, t);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        log.info("Container {} status received: {}", containerId, containerStatus);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        log.warn("Failed to get container {} status", containerId, t);
    }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
        log.warn("Unexpected update container resource event. Container: {}. Resource: {}", containerId, resource);
    }

    @Override
    public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {
        log.warn("Unexpected update container resource error event. Container: {}", containerId, t);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {
        log.warn("Unexpected container resource increased event. Container: {}. Resource: {}", containerId, resource);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {
        log.warn("Unexpected container resource increased error event. Container: {}.", containerId, t);
    }
}
