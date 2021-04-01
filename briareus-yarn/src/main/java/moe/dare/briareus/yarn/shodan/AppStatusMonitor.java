package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.common.concurrent.ThreadFactoryBuilder;
import moe.dare.briareus.common.utils.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static moe.dare.briareus.common.utils.Preconditions.checkState;

class AppStatusMonitor implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(AppStatusMonitor.class);
    private static final ThreadFactory THREAD_FACTORY = ThreadFactoryBuilder
            .withPrefix("yarn-application-status-monitor-")
            .deamon(true)
            .build();
    private static final String CLOSED_EXCEPTION_MSG = "Yarn application monitor closed";
    private static final int POLLING_INTERVAL_SECONDS = 5;

    private final Queue<Pair<ApplicationId, CompletableFuture<FinalApplicationStatus>>> applications = new ConcurrentLinkedQueue<>();
    private final UgiYarnClient yarnClient;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
    private final AtomicBoolean scheduled = new AtomicBoolean();

    AppStatusMonitor(UgiYarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    CompletableFuture<FinalApplicationStatus> monitorApplication(ApplicationId applicationId) {
        requireNonNull(applicationId, "applicationId");
        CompletableFuture<FinalApplicationStatus> cf = new CompletableFuture<>();
        Pair<ApplicationId, CompletableFuture<FinalApplicationStatus>> pair = Pair.of(applicationId, cf);
        checkState(!closed.get(), CLOSED_EXCEPTION_MSG);
        scheduleIfRequired();
        applications.add(pair);
        if (closed.get()) {
            applications.remove(pair);
            throw new IllegalStateException(CLOSED_EXCEPTION_MSG);
        }
        return cf;
    }

    private void checkStatuses() {
        Iterator<Pair<ApplicationId, CompletableFuture<FinalApplicationStatus>>> iterator = applications.iterator();
        while (iterator.hasNext()) {
            Pair<ApplicationId, CompletableFuture<FinalApplicationStatus>> p = iterator.next();
            ApplicationId applicationId = p.first();
            CompletableFuture<FinalApplicationStatus> future = p.second();
            if (future.isCancelled()) {
                log.debug("Application {} monitoring canceled", applicationId);
                iterator.remove();
            } else if (future.isDone() && !future.isCancelled()) {
                log.warn("Application {} monitoring future's completed outside monitor check. This may be an error.", applicationId);
                iterator.remove();
            } else {
                try {
                    ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
                    FinalApplicationStatus applicationStatus = applicationReport.getFinalApplicationStatus();
                    if (applicationStatus != null && applicationStatus != FinalApplicationStatus.UNDEFINED) {
                        log.info("Application {} completed with status: {}", applicationId, applicationStatus);
                        future.complete(applicationStatus);
                        iterator.remove();
                    } else {
                        log.debug("Current application {} status: {}", applicationId, applicationStatus);
                    }
                } catch (Exception e) {
                    log.warn("Can't get application {} report", applicationId, e);
                    break;
                }
            }
        }
        if (yarnClient.isStopped() && !closed.get()) {
            completeAllRemaining(new IllegalStateException("Yarn client stopped"));
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            es.shutdownNow();
            completeAllRemaining(new IllegalStateException(CLOSED_EXCEPTION_MSG));
        }
    }

    private void scheduleIfRequired() {
        if (scheduled.compareAndSet(false, true)) {
            es.scheduleAtFixedRate(this::checkStatuses, 0, POLLING_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    private void completeAllRemaining(Exception e) {
        Pair<ApplicationId, CompletableFuture<FinalApplicationStatus>> p;
        while ((p = applications.poll()) != null) {
            p.second().completeExceptionally(e);
        }
    }
}
