package moe.dare.briareus.local;

import moe.dare.briareus.api.BriareusContext;
import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.api.RemoteJvmProcess;
import moe.dare.briareus.common.concurrent.CancelToken;
import moe.dare.briareus.common.concurrent.CancelTokenSource;
import moe.dare.briareus.common.concurrent.TokenCanceledException;
import moe.dare.briareus.common.concurrent.CompletableFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static moe.dare.briareus.common.utils.Preconditions.checkState;

class BriareusLocalContext implements BriareusContext<RemoteJvmProcess> {
    private static final Logger log = LoggerFactory.getLogger(BriareusLocalContext.class);
    private static final String CLOSED_EXCEPTION_MSG = "Briareus " + BriareusLocalContext.class.getSimpleName() + " closed";
    private static final Duration DESTROY_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration FORCE_DESTROY_TIMEOUT = Duration.ofSeconds(5);

    private final Set<Path> workDirectories = ConcurrentHashMap.newKeySet();
    private final Set<Process> runningProcesses = ConcurrentHashMap.newKeySet();

    private final WorkDirectoryFactory workDirectoryFactory;
    private final ProcessMonitor processMonitor;
    private final Executor executor;

    private final CancelTokenSource closeOnCancelTokenSource = CancelTokenSource.newTokenSource();
    private final CancelToken closeToken = closeOnCancelTokenSource.token();

    BriareusLocalContext(WorkDirectoryFactory workDirectoryFactory, ProcessMonitor processMonitor, Executor executor) {
        this.workDirectoryFactory = requireNonNull(workDirectoryFactory, "working directory factory");
        this.processMonitor = requireNonNull(processMonitor, "process monitor");
        this.executor = requireNonNull(executor, "executor");
    }

    @Override
    public CompletableFuture<RemoteJvmProcess> start(RemoteJvmOptions options) {
        validateOptions(options);
        ensureNotClosed();
        CompletableFuture<State> workDirFuture = supplyAsync(() -> createWorkDirectory(State.newState(options)), executor);
        CompletableFuture<State> copyCompleteFuture = workDirFuture.thenApplyAsync(this::copyFiles, executor);
        CompletableFuture<State> processBuilder = copyCompleteFuture.thenApplyAsync(this::makeProcessBuilder, executor);
        CompletableFuture<State> processStarted = processBuilder.thenApplyAsync(this::startProcess, executor);
        CompletableFuture<State> processMonitoring = processStarted.thenApplyAsync(this::monitorProcess, executor);
        CompletableFuture<RemoteJvmProcess> result = processMonitoring.thenApplyAsync(this::toRemoteProcess, executor);
        result.whenCompleteAsync((remoteJvmProcess, throwable) -> {
            if (throwable != null) {
                Stream.of(processMonitoring, processStarted, processBuilder, copyCompleteFuture, workDirFuture) //reverse order
                        .map(CompletableFutures::getNow)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .findFirst()
                        .ifPresent(this::cleanupResources);
            } else {
                State lastState = processMonitoring.getNow(null);
                remoteJvmProcess.onExit()
                        .whenCompleteAsync((anyResult, anyException) -> cleanupResources(lastState), executor);
            }
        }, executor);
        return result.thenApply(Function.identity());
    }

    private void validateOptions(RemoteJvmOptions options) {
        requireNonNull(options, "JVM options");
    }

    private State createWorkDirectory(State state) {
        try {
            ensureNotClosed();
            Path path = requireNonNull(workDirectoryFactory.createDirectory(), "work directory");
            if (!workDirectories.add(path)) {
                throw new IllegalStateException("Work directory already in use: " + path);
            }
            if (closeToken.isCancellationRequested()) {
                if (workDirectories.remove(path)) {
                    deleteWorkDirectoryQuietly(path);
                }
                throw new IllegalStateException(CLOSED_EXCEPTION_MSG);
            }
            return state.withWorkDir(path);
        } catch (Exception e) {
            throw new BriareusException("Can't create working directory", e);
        }
    }

    private State copyFiles(State state) {
        try {
            new FileCopyTool(state.workDir).copy(state.options.files(), closeToken);
        } catch (TokenCanceledException canceled) {
            throw new BriareusException("Context closed", canceled);
        } catch (Exception e) {
            throw new BriareusException("Can't copy files", e);
        }
        return state;
    }

    private State makeProcessBuilder(State state) {
        final ProcessBuilder processBuilder = new ProcessBuilderFactory().create(state.options, state.workDir);
        return state.withProcessBuilder(processBuilder);
    }

    private State startProcess(State state) {
        ensureNotClosed();
        try {
            final Process process = state.processBuilder.start();
            runningProcesses.add(process);
            if (closeToken.isCancellationRequested()) {
                runningProcesses.remove(process);
                destroyProcess(process);
                closeToken.throwIfCancellationRequested();
            }
            return state.withProcess(process);
        } catch (IOException e) {
            throw new BriareusException("Can't start process", e);
        }
    }

    private State monitorProcess(State state) {
        ensureNotClosed();
        return state.withTerminateFuture(processMonitor.monitorProcess(state.process));
    }

    private RemoteJvmProcess toRemoteProcess(State state) {
        return new LocalJvmProcess(state.process, state.terminateFuture);
    }

    public void cleanupResources(State state) {
        destroyProcess(state.process);
        deleteWorkDirectoryQuietly(state.workDir);
    }

    private void destroyProcess(Process process) {
        if (process == null) {
            return;
        }
        boolean reInterrupt = false;
        try {
            process.destroy();
            try {
                if (process.waitFor(DESTROY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    runningProcesses.remove(process);
                    return;
                }
                log.warn("Process {} didn't respond to destroy for {}", process, DESTROY_TIMEOUT);
            } catch (InterruptedException e) {
                log.warn("Interrupted during destroy process", e);
                reInterrupt = true;
            }
            process.destroyForcibly();
            try {
                if (process.waitFor(FORCE_DESTROY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    runningProcesses.remove(process);
                    return;
                }
                log.error("Process {} didn't respond to force destroy for {}", process, FORCE_DESTROY_TIMEOUT);
            } catch (InterruptedException e) {
                log.warn("Interrupted during force destroy process", e);
                reInterrupt = true;
            }
        } finally {
            if (reInterrupt) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void deleteWorkDirectoryQuietly(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (exc != null) {
                        throw exc;
                    }
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            workDirectories.remove(path);
        } catch (Exception e) {
            log.error("Can't clear work dir {}", path, e);
        }
    }

    @Override
    public void close() {
        closeOnCancelTokenSource.cancel();
        while (!runningProcesses.isEmpty()) {
            List<Process> toStop = new ArrayList<>(runningProcesses);
            toStop.forEach(runningProcesses::remove);
            toStop.forEach(this::destroyProcess);
        }
        while (!workDirectories.isEmpty()) {
            List<Path> toRemove = new ArrayList<>(workDirectories);
            toRemove.forEach(workDirectories::remove);
            toRemove.forEach(this::deleteWorkDirectoryQuietly);
        }
    }

    private void ensureNotClosed() {
        checkState(!closeToken.isCancellationRequested(), CLOSED_EXCEPTION_MSG);
    }

    private static final class State {
        private final RemoteJvmOptions options;
        private final Path workDir;
        private final ProcessBuilder processBuilder;
        private final Process process;
        private final CompletableFuture<?> terminateFuture;

        private static State newState(RemoteJvmOptions options) {
            return new Builder(options).build();
        }

        public State withWorkDir(Path workDir) {
            return new Builder(this).workDir(workDir).build();
        }

        public State withProcessBuilder(ProcessBuilder processBuilder) {
            return new Builder(this).processBuilder(processBuilder).build();
        }

        public State withProcess(Process process) {
            return new Builder(this).process(process).build();
        }

        public State withTerminateFuture(CompletableFuture<?> terminateFuture) {
            return new Builder(this).terminateFuture(terminateFuture).build();
        }

        private State(Builder builder) {
            this.options = builder.options;
            this.workDir = builder.workDir;
            this.processBuilder = builder.processBuilder;
            this.process = builder.process;
            this.terminateFuture = builder.terminateFuture;
        }

        private static final class Builder {
            private final RemoteJvmOptions options;
            private Path workDir;
            private ProcessBuilder processBuilder;
            private Process process;
            private CompletableFuture<?> terminateFuture;

            private Builder(State state) {
                this.options = state.options;
                this.workDir = state.workDir;
                this.processBuilder = state.processBuilder;
                this.process = state.process;
                this.terminateFuture = state.terminateFuture;
            }

            private Builder(RemoteJvmOptions options) {
                this.options = options;
            }

            private State build() {
                return new State(this);
            }

            public Builder workDir(Path workDir) {
                this.workDir = workDir;
                return this;
            }

            public Builder processBuilder(ProcessBuilder processBuilder) {
                this.processBuilder = processBuilder;
                return this;
            }

            public Builder process(Process process) {
                this.process = process;
                return this;
            }

            public Builder terminateFuture(CompletableFuture<?> terminateFuture) {
                this.terminateFuture = terminateFuture;
                return this;
            }
        }
    }
}
