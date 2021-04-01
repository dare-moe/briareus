package moe.dare.briareus.yarn.sensei;

import com.google.common.util.concurrent.AtomicDouble;
import moe.dare.briareus.api.*;
import moe.dare.briareus.common.concurrent.ThreadFactoryBuilder;
import moe.dare.briareus.common.utils.Either;
import moe.dare.briareus.yarn.launch.LaunchContextFactory;
import moe.dare.briareus.yarn.reousrces.ResourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

class BriareusYarnSenseiContextImpl implements BriareusYarnSenseiContext {
    private static final Logger log = LoggerFactory.getLogger(BriareusYarnSenseiContextImpl.class);
    private static final ExecutionTypeRequest GUARANTEED_EXECUTION_TYPE = ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true);
    private static final ThreadFactory HEARTBEAT_THREAD_FACTORY = ThreadFactoryBuilder
            .withPrefix("briareus-yarn-sensei-heartbeat-")
            .deamon(false)
            .build();
    private static final String CONTEXT_CLOSED_MSG = "Briareus Sensei context closed";

    private final ConcurrentMap<Long, CompletableFuture<Container>> allocatingContainers = new ConcurrentHashMap<>();
    private final ConcurrentMap<ContainerId, CompletableFuture<Void>> startingContainers = new ConcurrentHashMap<>();
    private final ConcurrentMap<ContainerId, CompletableFuture<Integer>> exitCodes = new ConcurrentHashMap<>();
    private final AMRMClient<ContainerRequest> amrmClient;
    private final NMClientAsync nmClientAsync;
    private final AtomicInteger requestCounter = new AtomicInteger();
    private final AtomicDouble progress = new AtomicDouble();

    private final UserGroupInformation user;
    private final LaunchContextFactory launchContextFactory;
    private final ResourceFactory resourceFactory;
    private final Runnable shutdownRequestHandler;

    private volatile ApplicationStatus finalStatus = ApplicationStatus.succeeded();
    private volatile Resource maximumResourceCapability;
    private volatile ScheduledExecutorService heartBeat;
    private volatile boolean closed;

    BriareusYarnSenseiContextImpl(UserGroupInformation user,
                                  LaunchContextFactory launchContextFactory,
                                  ResourceFactory resourceFactory,
                                  Runnable shutdownRequestHandler) {
        this.user = requireNonNull(user, "user");
        this.launchContextFactory = requireNonNull(launchContextFactory, "launchContextFactory");
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory");
        this.shutdownRequestHandler = requireNonNull(shutdownRequestHandler, "shutdownRequestHandler");
        NMTokenCache nmTokenCache = new NMTokenCache(); // get rid of NMTokenCache singleton
        NMCallbackHandler nmCallback = new NMCallbackHandler(startingContainers);
        amrmClient = user.doAs((PrivilegedAction<AMRMClient<ContainerRequest>>)AMRMClient::createAMRMClient);
        nmClientAsync = user.doAs((PrivilegedAction<NMClientAsync>)() -> NMClientAsync.createNMClientAsync(nmCallback));
        amrmClient.setNMTokenCache(nmTokenCache);
        nmClientAsync.getClient().setNMTokenCache(nmTokenCache);
    }

    void startContext(Configuration configuration, String host, int port, String url) {
        requireNonNull(configuration, "configuration");
        requireNonNull(host, "host");
        heartBeat = Executors.newSingleThreadScheduledExecutor(HEARTBEAT_THREAD_FACTORY);
        user.doAs((PrivilegedAction<Void>) () -> {
            try {
                amrmClient.init(configuration);
                amrmClient.start();
                nmClientAsync.init(configuration);
                nmClientAsync.start();
                RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster(host, port, url);
                maximumResourceCapability = response.getMaximumResourceCapability();
            } catch (Exception e) {
                heartBeat.shutdownNow();
                BriareusException startFailedException = new BriareusException("Can't start Briareus Sensei", e);
                for (AbstractService service : Arrays.asList(amrmClient, nmClientAsync)) {
                    try {
                        service.stop();
                    } catch (Exception stopEx) {
                        startFailedException.addSuppressed(stopEx);
                    }
                }
                throw startFailedException;
            }
            return null;
        });
        heartBeat.scheduleAtFixedRate(this::heartbeatYarn, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public CompletionStage<RemoteJvmProcess> start(RemoteJvmOptions options) {
        ensureNotClosed();
        verifyOptions(options);
        ContainerRequest request = createRequest(options);
        CompletionStage<ContainerLaunchContext> launchContextFuture = launchContextFactory.create(options);
        CompletableFuture<Container> containerFuture = allocateContainer(request);
        return launchContextFuture.handle(Either::oneOfNullable).thenCombine(containerFuture, (context, container) -> {
            if (context.isRight()) {
                log.warn("Stopping container {} before start", container.getId());
                amrmClient.releaseAssignedContainer(container.getId());
                throw new JvmStartFailedException("Can't prepare container context", context.right());
            }
            return launchContainer(container, context.left());
        }).thenCompose(future -> future).thenApply(x -> x);
    }

    private void verifyOptions(RemoteJvmOptions options) {
        requireNonNull(options);
    }

    private ContainerRequest createRequest(RemoteJvmOptions options) {
        int requestNumber = requestCounter.updateAndGet(x -> Math.max(x + 1, 1));
        Resource resources = resourceFactory.resources(options, maximumResourceCapability);
        return ContainerRequest.newBuilder()
                .allocationRequestId(requestNumber)
                .executionTypeRequest(GUARANTEED_EXECUTION_TYPE)
                .capability(resources)
                .priority(Priority.newInstance(requestNumber))
                .build();
    }

    private CompletableFuture<Container> allocateContainer(ContainerRequest request) {
        CompletableFuture<Container> containerFuture = new CompletableFuture<>();
        if (allocatingContainers.putIfAbsent(request.getAllocationRequestId(), containerFuture) != null) {
            throw new IllegalStateException("Allocation id " + request.getAllocationRequestId() + " already registered");
        }
        amrmClient.addContainerRequest(request);
        return containerFuture;
    }

    private CompletableFuture<RemoteJvmProcess> launchContainer(Container container, ContainerLaunchContext context) {
        final ContainerId containerId = container.getId();
        final NodeId nodeId = container.getNodeId();
        try {
            CompletableFuture<Void> startedFuture = new CompletableFuture<>();
            CompletableFuture<Integer> exitCodeFuture = new CompletableFuture<>();
            startingContainers.put(containerId, startedFuture);
            exitCodes.put(containerId, exitCodeFuture);
            nmClientAsync.startContainerAsync(container, context);
            startedFuture.exceptionally(throwable -> {
                exitCodes.remove(containerId);
                exitCodeFuture.completeExceptionally(throwable);
                return null;
            });
            return startedFuture.thenApply(any -> new YarnContainerJvmProcess(nmClientAsync, containerId, nodeId, exitCodeFuture));
        } catch (Exception e) {
            log.warn("Failed to start container container {}. Releasing.", containerId);
            amrmClient.releaseAssignedContainer(containerId);
            throw e;
        }
    }

    @Override
    public synchronized void setFinalStatus(ApplicationStatus status) {
        requireNonNull(status, "status");
        ensureNotClosed();
        this.finalStatus = status;
    }

    @Override
    public void setProgress(double progress) {
        if (progress < 0 || progress > 1) {
            throw new IllegalArgumentException("Progress must be in range [0, 1], but was " + progress);
        }
        ensureNotClosed();
        this.progress.set(progress);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        List<Exception> exceptions = new ArrayList<>();
        try {
            nmClientAsync.stop();
        } catch (Exception e) {
            log.error("Node manager client stopped with errors");
            exceptions.add(e);
        }
        heartBeat.shutdownNow();
        try {
            heartBeat.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Interrupted during awaiting for hearBeatThread completion");
            Thread.currentThread().interrupt();
        }
        if (!heartBeat.isTerminated()) {
            log.error("Heartbeat thread is not terminated normally");
            exceptions.add(new IllegalStateException("Heartbeat thread not terminated"));
        }
        try {
            FinalApplicationStatus finalApplicationStatus = finalStatus.getFinalApplicationStatus();
            String message = finalStatus.getMessage();
            String newTrackUrl = finalStatus.getNewTrackUrl();
            amrmClient.unregisterApplicationMaster(finalApplicationStatus, message, newTrackUrl);
        } catch (Exception e) {
            log.error("Can't unregister from YARN");
            exceptions.add(e);
        }
        try {
            amrmClient.stop();
        } catch (Exception e) {
            log.error("Can't stop amrm client");
            exceptions.add(e);
        }
        if (!exceptions.isEmpty()) {
            BriareusException contextCloseException = new BriareusException("Context not closed properly", exceptions.get(0));
            exceptions.stream().skip(1).forEachOrdered(contextCloseException::addSuppressed);
            throw contextCloseException;
        }
    }

    private void heartbeatYarn() {
        Either<AllocateResponse, Exception> either = user.doAs((PrivilegedAction<Either<AllocateResponse, Exception>>) () -> {
            try {
                return Either.left(amrmClient.allocate(((float) progress.get())));
            } catch (Exception e) {
                return Either.right(e);
            }
        });
        if (either.isLeft()) {
            processHeartBeatResponse(either.left());
        } else {
            processHeartBeatException(either.right());
        }
    }

    private void processHeartBeatResponse(AllocateResponse response) {
        try {
            response.getAllocatedContainers().forEach(this::processAllocatedContainer);
            response.getCompletedContainersStatuses().forEach(this::processCompletedContainer);
        } catch (Exception e) {
            log.error("Exception processing allocate response.", e);
        }
    }

    private void processAllocatedContainer(Container allocatedContainer) {
        long requestId = allocatedContainer.getAllocationRequestId();
        if (requestId <= 0) {
            requestId = allocatedContainer.getPriority().getPriority();
        }
        CompletableFuture<Container> allocatedFuture = allocatingContainers.remove(requestId);
        if (allocatedFuture != null) {
            allocatedFuture.complete(allocatedContainer);
            ArrayList<ContainerRequest> requests = new ArrayList<>(amrmClient.getMatchingRequests(requestId));
            requests.forEach(amrmClient::removeContainerRequest);
        } else {
            log.warn("Unknown allocation request id {}. Releasing container {}.", requestId, allocatedContainer.getId());
            amrmClient.releaseAssignedContainer(allocatedContainer.getId());
        }
    }

    private void processCompletedContainer(ContainerStatus completedContainer) {
        ContainerId containerId = completedContainer.getContainerId();
        CompletableFuture<Integer> exitCodeFuture = exitCodes.remove(containerId);
        if (exitCodeFuture != null) {
            exitCodeFuture.complete(completedContainer.getExitStatus());
        } else {
            log.error("Untracked container {} completed.", containerId);
        }
    }

    private void processHeartBeatException(Exception exception) {
        if (exception instanceof ApplicationAttemptNotFoundException) {
            log.warn("Resource manager asked Sensei to shutdown");
            shutdownRequestHandler.run();
        } else if (exception instanceof IOException) {
            log.warn("Io exception during YARN heartbeat. Possible RPC problems", exception);
        } else {
            log.error("Exception during heartbeat.", exception);
        }
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException(CONTEXT_CLOSED_MSG);
        }
    }
}
