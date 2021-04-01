package moe.dare.briareus.local;

import moe.dare.briareus.common.utils.Pair;
import moe.dare.briareus.common.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.*;

import static java.util.Objects.requireNonNull;
import static moe.dare.briareus.common.utils.Preconditions.checkArgument;
import static moe.dare.briareus.common.utils.Preconditions.checkState;

public class PollingProcessMonitor implements ProcessMonitor {
    private static final Logger log = LoggerFactory.getLogger(PollingProcessMonitor.class);
    private static final ThreadFactory DEFAULT_THREAD_FACTORY = ThreadFactoryBuilder
            .withPrefix("briareus-process-monitor-")
            .deamon(true)
            .build();
    private static final String CLOSED_EXCEPTION_MSG = "Polling process monitor closed";
    private static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(1);
    private static final Duration INTERVAL_UPPER_BOUND = Duration.ofMillis(Long.MAX_VALUE);
    private static final Duration INTERVAL_LOWER_BOUND = Duration.ofMillis(1);

    private final Queue<Pair<Process, CompletableFuture<Void>>> processes = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService es;
    private final Duration interval;
    private final Object closeLock = new Object();
    private volatile boolean closed;

    public static ProcessMonitor create() {
        return create(DEFAULT_POLLING_INTERVAL);
    }

    public static ProcessMonitor create(Duration pollInterval) {
        return create(pollInterval, DEFAULT_THREAD_FACTORY);
    }

    public static ProcessMonitor create(Duration pollInterval, ThreadFactory threadFactory) {
        return new PollingProcessMonitor(pollInterval, threadFactory);
    }

    private PollingProcessMonitor(Duration pollInterval, ThreadFactory threadFactory) {
        requireNonNull(threadFactory, "thread factory");
        requireNonNull(pollInterval, "poll interval");
        checkArgument(!pollInterval.isZero() && !pollInterval.isNegative(), "poll interval must be greater then 0");
        if (INTERVAL_LOWER_BOUND.compareTo(pollInterval) < 0) {
            this.interval = INTERVAL_LOWER_BOUND;
        } else if (INTERVAL_UPPER_BOUND.compareTo(pollInterval) > 0) {
            this.interval = INTERVAL_UPPER_BOUND;
        } else {
            this.interval = pollInterval;
        }
        this.es = Executors.newSingleThreadScheduledExecutor(threadFactory);
        es.scheduleAtFixedRate(this::checkStatuses, 0, this.interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> monitorProcess(Process process) {
        requireNonNull(process, "process");
        checkState(!closed, CLOSED_EXCEPTION_MSG);
        CompletableFuture<Void> cf = new CompletableFuture<>();
        synchronized (closeLock) {
            checkState(!closed, CLOSED_EXCEPTION_MSG);
            processes.add(Pair.of(process, cf));
        }
        return cf;
    }

    private void checkStatuses() {
        for (Iterator<Pair<Process, CompletableFuture<Void>>> iterator = processes.iterator(); iterator.hasNext(); ) {
            Pair<Process, CompletableFuture<Void>> p = iterator.next();
            Process process = p.first();
            CompletableFuture<Void> future = p.second();
            if (future.isCancelled()) {
                log.debug("Process {} monitoring canceled", process);
                iterator.remove();
            } else if (future.isDone() && !future.isCancelled()) {
                log.warn("Process {} monitoring future's completed outside monitor check. This may be an error.", process);
                iterator.remove();
            } else if (!process.isAlive()) {
                log.info("Process {} completed", process);
                future.complete(null);
                iterator.remove();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        es.shutdownNow();
        checkStatuses();
        synchronized (closeLock) {
            CancellationException exception = null;
            for (Pair<Process, CompletableFuture<Void>> pair = processes.poll(); pair != null; pair = processes.poll()) {
                if (exception == null) {
                    exception = new CancellationException(CLOSED_EXCEPTION_MSG);
                }
                pair.second().completeExceptionally(exception);
            }
        }
    }

    @Override
    public String toString() {
        return "PollingProcessMonitor{interval=" + interval + '}';
    }
}
