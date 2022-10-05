package moe.dare.briareus.local;

import moe.dare.briareus.api.RemoteJvmProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class LocalJvmProcess implements RemoteJvmProcess {
    private static final Logger log = LoggerFactory.getLogger(LocalJvmProcess.class);

    private static final Function<Process, Long> pidAccessor = createPidAccessor();

    private final Process process;
    private final CompletableFuture<?> terminatedFuture;
    private final Long pid;

    static LocalJvmProcess create(Process process, CompletableFuture<?> terminatedFuture) {
        return new LocalJvmProcess(process, terminatedFuture, pidAccessor.apply(process));
    }

    private LocalJvmProcess(Process process, CompletableFuture<?> terminatedFuture, Long pid) {
        this.process = requireNonNull(process);
        this.terminatedFuture = requireNonNull(terminatedFuture);
        this.pid = pid;
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
        return process.isAlive() ? OptionalInt.empty() : OptionalInt.of(process.exitValue());
    }

    @Override
    public CompletionStage<RemoteJvmProcess> onExit() {
        return terminatedFuture.thenApply(any -> this);
    }

    /**
     * @return pid of process or null.
     */
    @Override
    public Object getExternalId() {
        return pid;
    }

    private static Function<Process, Long> createPidAccessor() {
        try {
            Method pidMethod = Process.class.getMethod("pid");
            return process -> getPidUsingMethod(pidMethod, process);
        } catch (Exception e) {
            log.info("Method 'pid()' for Process class not found");
        }
        return LocalJvmProcess::getUnixPid;
    }

    private static Long getPidUsingMethod(Method method, Process process) {
        try {
            Object pid = method.invoke(process);
            return pid instanceof Long ? (Long) pid : null;
        } catch (Exception e) {
            log.error("Unable to invoke {} for {} instance", method.getName(), process.getClass().getName());
            return null;
        }
    }

    private static Long getUnixPid(Process process) {
        try {
            Field f = process.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            return f.getLong(process);
        } catch (Exception e) {
            log.error("Unable to get pid-field value for {} instance", process.getClass());
            return null;
        }
    }
}
