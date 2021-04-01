package moe.dare.briareus.common;

import moe.dare.briareus.api.RemoteJvmOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public enum JvmArgsFactory {
    WINDOWS(";"),
    LINUX(":");

    private final String classPathDelimiter;

    JvmArgsFactory(String classPathDelimiter) {
        this.classPathDelimiter = classPathDelimiter;
    }

    public List<String> createJvmArgs(RemoteJvmOptions options) {
        List<String> args = new ArrayList<>(options.vmOptions().size() + options.arguments().size() + 5);
        args.addAll(options.vmOptions());
        args.addAll(makeClasspathArgument(options));
        args.addAll(makeXmxArgument(options));
        args.add(options.mainClass());
        args.addAll(options.arguments());
        return args;
    }

    private List<String> makeXmxArgument(RemoteJvmOptions options) {
        OptionalLong heapSize = options.maxHeapSize();
        if (heapSize.isPresent()) {
            String arg = "-Xmx" + heapSize.getAsLong();
            return singletonList(arg);
        }
        return emptyList();
    }

    private List<String> makeClasspathArgument(RemoteJvmOptions options) {
        if (options.classpath().isEmpty()) {
            return emptyList();
        }
        String classpathArg = String.join(classPathDelimiter, options.classpath());
        return asList("-cp", classpathArg);
    }
}
