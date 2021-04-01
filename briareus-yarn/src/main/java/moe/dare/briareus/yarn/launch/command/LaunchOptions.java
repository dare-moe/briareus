package moe.dare.briareus.yarn.launch.command;

import moe.dare.briareus.api.FileEntry;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Describes command and environment to actually start JVM.
 * This is subset of {@link org.apache.hadoop.yarn.api.records.ContainerLaunchContext}
 */
public final class LaunchOptions {
    private final List<FileEntry> launcherFiles;
    private final Map<String, String> environment;
    private final List<String> command;

    /**
     * Constructs launch options
     *
     * @param launcherFiles additional files required to run command.
     * @param environment environment raw variables
     * @param command command
     * @return LaunchOptions instance
     */
    public static LaunchOptions create(List<FileEntry> launcherFiles, Map<String, String> environment, List<String> command) {
        return new LaunchOptions(launcherFiles, environment, command);
    }

    private LaunchOptions(List<FileEntry> launcherFiles, Map<String, String> environment, List<String> command) {
        this.launcherFiles = requireNonNull(launcherFiles, "launcherFiles");
        this.environment = requireNonNull(environment, "environment");
        this.command = requireNonNull(command, "command");
    }

    /**
     * @return files required to execute command
     */
    public List<FileEntry> launcherFiles() {
        return launcherFiles;
    }

    /**
     * @return <em>environment variables</em> for the container.
     */
    public Map<String, String> environment() {
        return environment;
    }

    /**
     * @return command which will start JVM.
     */
    public List<String> command() {
        return command;
    }
}
