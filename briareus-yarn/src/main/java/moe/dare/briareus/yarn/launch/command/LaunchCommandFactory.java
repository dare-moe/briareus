package moe.dare.briareus.yarn.launch.command;

import moe.dare.briareus.api.RemoteJvmOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;

/**
 * Factory responsible for generating launch options.
 *
 * @see LaunchOptions
 * @see org.apache.hadoop.yarn.api.records.ContainerLaunchContext#getCommands()
 */
public interface LaunchCommandFactory extends Closeable {
    /**
     * Creates default launch command factory.
     *
     * @param user user running application.
     * @param conf yarn cluster hadoop configuration.
     * @return default Launch Command Factory implementation.
     */
    static LaunchCommandFactory createDefault(UserGroupInformation user, Configuration conf) {
        return DefaultCommandFactory.create(user, conf);
    }

    /**
     * @param jvmOptions jvm options for new container.
     * @return launch options for remote jvm.
     */
    LaunchOptions createLaunchOptions(RemoteJvmOptions jvmOptions);

    @Override
    default void close() {
    }
}
