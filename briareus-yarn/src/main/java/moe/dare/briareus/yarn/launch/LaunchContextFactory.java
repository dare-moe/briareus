package moe.dare.briareus.yarn.launch;

import moe.dare.briareus.api.RemoteJvmOptions;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

import java.util.concurrent.CompletionStage;

/**
 * Factory creates {@link ContainerLaunchContext} based on JVM options.
 * @see DefaultLaunchContextFactory
 */
public interface LaunchContextFactory {
    CompletionStage<ContainerLaunchContext> create(RemoteJvmOptions jvmOptions);
}
