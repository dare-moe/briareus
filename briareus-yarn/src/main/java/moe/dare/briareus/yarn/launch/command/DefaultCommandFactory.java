package moe.dare.briareus.yarn.launch.command;

import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.FileSource;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.common.JvmArgsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.Objects.requireNonNull;
import static moe.dare.briareus.api.FileSources.classpathSource;

class DefaultCommandFactory implements LaunchCommandFactory {
    private static final Logger log = LoggerFactory.getLogger(DefaultCommandFactory.class);
    private static final FileSource START_SCRIPT = classpathSource(DefaultCommandFactory.class.getClassLoader(),
            "moe/dare/briareus/yarn/launch/briareus_launcher.sh");
    private static final String SHELL_COMMAND = "bash";
    private static final String START_SCRIPT_REMOTE_NAME = ".briareus_launcher";
    private static final FileEntry START_SCRIPT_ENTRY = FileEntry.copy(START_SCRIPT, START_SCRIPT_REMOTE_NAME);
    private static final List<FileEntry> ADDITIONAL_RESOURCES = Collections.singletonList(START_SCRIPT_ENTRY);
    private static final JvmArgsFactory ARGS_FACTORY = JvmArgsFactory.LINUX;
    private static final String HADOOP_USER_NAME_ENV_VAR = "HADOOP_USER_NAME";
    private final String userName;
    private final Configuration conf;

    static LaunchCommandFactory create(UserGroupInformation user, Configuration conf) {
        return new DefaultCommandFactory(user.getShortUserName(), conf);
    }

    private DefaultCommandFactory(String userName, Configuration conf) {
        this.userName = requireNonNull(userName, "user");
        this.conf = requireNonNull(conf, "conf");
    }

    @Override
    public LaunchOptions createLaunchOptions(RemoteJvmOptions jvmOptions) {
        Map<String, String> environment = new LinkedHashMap<>(jvmOptions.environmentOverrides());
        if (SecurityUtil.getAuthenticationMethod(conf) == AuthenticationMethod.SIMPLE &&
                !environment.containsKey(HADOOP_USER_NAME_ENV_VAR)) {
            log.debug("Setting {} environment variable for simple auth to '{}'", HADOOP_USER_NAME_ENV_VAR, userName);
            environment.put(HADOOP_USER_NAME_ENV_VAR, userName);
        }
        List<String> command = createCommand(jvmOptions);
        return LaunchOptions.create(ADDITIONAL_RESOURCES, environment, command);
    }

    private List<String> createCommand(RemoteJvmOptions jvmOptions) {
        List<String> args = ARGS_FACTORY.createJvmArgs(jvmOptions);
        List<String> command = new ArrayList<>(args.size() + 5);
        command.add(SHELL_COMMAND);
        command.add(START_SCRIPT_REMOTE_NAME);
        for (String arg : args) {
            command.add(encodeBase64(arg));
        }
        command.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout.log");
        command.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr.log");
        command.add("3>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/briareus_launcher.log");
        return Collections.singletonList(String.join(" ", command));
    }

    private static String encodeBase64(String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }
}
