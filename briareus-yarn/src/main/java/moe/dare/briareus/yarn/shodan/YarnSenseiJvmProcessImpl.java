package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.api.RemoteJvmProcess;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class YarnSenseiJvmProcessImpl implements YarnSenseiJvmProcess {
    private final ApplicationId applicationId;
    private final UgiYarnClient client;
    private final CompletableFuture<FinalApplicationStatus> status;

    YarnSenseiJvmProcessImpl(ApplicationId applicationId, UgiYarnClient yarnClient, CompletableFuture<FinalApplicationStatus> status) {
        this.applicationId = applicationId;
        this.client = yarnClient;
        this.status = status;
    }

    @Override
    public FinalApplicationStatus completionStatus() {
        return status.getNow(FinalApplicationStatus.UNDEFINED);
    }

    @Override
    public void destroy() {
        try {
            client.killApplication(applicationId);
        } catch (Exception e) {
            throw new BriareusException("Can't kill application", e);
        }
    }

    @Override
    public void destroyForcibly() {
        destroy();
    }

    @Override
    public boolean isAlive() {
        return !status.isDone();
    }

    @Override
    public OptionalInt exitCode() {
        return OptionalInt.empty();
    }

    @Override
    public CompletionStage<RemoteJvmProcess> onExit() {
        return status.thenApply(any -> this);
    }
}
