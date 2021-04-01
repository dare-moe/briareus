package moe.dare.briareus.local;

import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.common.JvmArgsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ProcessBuilderFactory {
    private static final String JAVA_HOME = "JAVA_HOME";
    private static final Logger log = LoggerFactory.getLogger(ProcessBuilderFactory.class);
    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().startsWith("windows");
    private static final File NULL_FILE = new File(IS_WINDOWS ? "NUL" : "/dev/null");
    private static final JvmArgsFactory ARGS_FACTORY = IS_WINDOWS? JvmArgsFactory.WINDOWS : JvmArgsFactory.LINUX;
    private static final String JAVA_HOME_EXEC = "bin/java";
    private static final String FALLBACK_JAVA_EXEC;
    static {
        String javaHomeProp = System.getProperty("java.home");
        if (javaHomeProp != null) {
            log.debug("Using java.home system property as fallback java home");
            FALLBACK_JAVA_EXEC = Paths.get(javaHomeProp, JAVA_HOME_EXEC).toString();
        } else {
            log.warn("System property 'java.home' is unset. Using simple 'java' string as fallback");
            FALLBACK_JAVA_EXEC = "java";
        }
    }


    ProcessBuilder create(RemoteJvmOptions options, Path workDir) {
        ProcessBuilder pb = new ProcessBuilder();
        pb.directory(workDir.toFile());
        setupEnvironment(pb, options.environmentOverrides());
        setupCommand(pb, options);
        setupRedirects(pb);
        return pb;
    }

    private void setupEnvironment(ProcessBuilder processBuilder, Map<String, String> overrides) {
        Map<String, String> processEnv = processBuilder.environment();
        overrides.forEach((key, value) -> {
            if (value == null) {
                processEnv.remove(key);
            } else {
                processEnv.put(key, value);
            }
        });
    }

    private void setupCommand(ProcessBuilder processBuilder, RemoteJvmOptions options) {
        List<String> args = ARGS_FACTORY.createJvmArgs(options);
        String javaExec = detectJava(processBuilder.environment());
        List<String> command = new ArrayList<>(args.size() + 1);
        command.add(javaExec);
        command.addAll(args);
        processBuilder.command(command);
    }

    private String detectJava(Map<String, String> jvmEnv) {
        String javaHome = jvmEnv.get(JAVA_HOME);
        if (javaHome != null && !javaHome.isEmpty()) {
            log.debug("Will use {} environment variable as java home dir", JAVA_HOME);
            return Paths.get(javaHome, JAVA_HOME_EXEC).toString();
        } else {
            log.info("Java home not set. Using fallback java command: '{}'", FALLBACK_JAVA_EXEC);
            return FALLBACK_JAVA_EXEC;
        }
    }

    private void setupRedirects(ProcessBuilder pb) {
        pb.redirectError(NULL_FILE);
        pb.redirectOutput(NULL_FILE);
    }
}