package moe.dare.briareus.local;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

public interface ProcessMonitor extends Closeable {
    /**
     * @param process to monitor
     * @return future that will be completed when Process terminated
     */
    CompletableFuture<Void> monitorProcess(Process process);

    static ProcessMonitor createDefault() {
        return PollingProcessMonitor.create();
    }
}
