package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.common.concurrent.ThreadFactoryBuilder;
import moe.dare.briareus.yarn.launch.LaunchContextFactory;
import moe.dare.briareus.yarn.reousrces.ResourceFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.concurrent.*;

import static java.util.Objects.requireNonNull;
import static moe.dare.briareus.common.utils.Preconditions.checkState;

class BriareusYarnShodanContextImpl implements BriareusYarnShodanContext {
    private static final String YARN_APPLICATION_TYPE = "BRIAREUS";
    private static final String DEFAULT_APPLICATION_NAME = "Briareus application";
    private static final int DEFAULT_APPLICATION_ATTEMPTS = 1;
    private static final ThreadFactory STARTER_THREAD_FACTORY = ThreadFactoryBuilder
            .withPrefix("briareus-yarn-shodan-starter-")
            .deamon(false)
            .build();
    private static final String CONTEXT_CLOSED_MSG = "Shodan context closed";

    private final ExecutorService startContainerExecutor = Executors.newCachedThreadPool(STARTER_THREAD_FACTORY);
    private final LaunchContextFactory launchContextFactory;
    private final ResourceFactory resourceFactory;
    private final UgiYarnClient client;
    private final AppStatusMonitor appStatusMonitor;
    private volatile boolean closed;

    public BriareusYarnShodanContextImpl(UgiYarnClient client, LaunchContextFactory launchContextFactory, ResourceFactory resourceFactory) {
        this.client = client;
        this.launchContextFactory = launchContextFactory;
        this.resourceFactory = resourceFactory;
        this.appStatusMonitor = new AppStatusMonitor(client);
    }

    @Override
    public CompletionStage<YarnSenseiJvmProcess> start(RemoteJvmOptions options) {
        checkState(!closed, CONTEXT_CLOSED_MSG);
        verifyOptions(options);
        CompletionStage<ContainerLaunchContext> context = launchContextFactory.create(options);
        return context.thenApplyAsync(c -> start(options, c), startContainerExecutor).thenApply(x -> x);
    }

    private void verifyOptions(RemoteJvmOptions options) {
        requireNonNull(options);
    }

    private YarnSenseiJvmProcess start(RemoteJvmOptions options, ContainerLaunchContext containerLaunchContext) {
        YarnClientApplication app = client.createApplication();
        Resource maximumResourceCapability = app.getNewApplicationResponse().getMaximumResourceCapability();
        Resource senseiContainerResource = resourceFactory.resources(options, maximumResourceCapability);
        ApplicationSubmissionContext senseiContext = app.getApplicationSubmissionContext();
        senseiContext.setApplicationType(YARN_APPLICATION_TYPE);
        senseiContext.setAMContainerSpec(containerLaunchContext);
        senseiContext.setResource(senseiContainerResource);
        senseiContext.setKeepContainersAcrossApplicationAttempts(keepContainers(options));
        senseiContext.setApplicationName(applicationName(options));
        senseiContext.setMaxAppAttempts(applicationAttempts(options));
        senseiContext.setQueue(yarnQueue(options));
        senseiContext.setPriority(applicationPriority(options));
        senseiContext.setUnmanagedAM(false);
        ApplicationId applicationId = client.submitApplication(senseiContext);
        CompletableFuture<FinalApplicationStatus> statusFuture = appStatusMonitor.monitorApplication(applicationId);
        return new YarnSenseiJvmProcessImpl(applicationId, client, statusFuture);
    }

    private boolean keepContainers(RemoteJvmOptions options) {
        return options.getOpt(ShodanOpts.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS).orElse(false);
    }

    private String applicationName(RemoteJvmOptions options) {
        return options.getOpt(ShodanOpts.YARN_APPLICATION_NAME).orElse(DEFAULT_APPLICATION_NAME);
    }

    private int applicationAttempts(RemoteJvmOptions options) {
        return options.getOpt(ShodanOpts.YARN_APPLICATION_ATTEMPTS).orElse(DEFAULT_APPLICATION_ATTEMPTS);
    }

    private String yarnQueue(RemoteJvmOptions options) {
        return options.getOpt(ShodanOpts.YARN_QUEUE).orElse(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }

    private Priority applicationPriority(RemoteJvmOptions options) {
        return options.getOpt(ShodanOpts.YARN_APPLICATION_PRIORITY).map(Priority::newInstance).orElse(Priority.UNDEFINED);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        startContainerExecutor.shutdownNow();
        appStatusMonitor.close();
        try {
            client.stop();
        } catch (Exception e) {
            throw new BriareusException("Can't close yarn client", e);
        }
    }
}
