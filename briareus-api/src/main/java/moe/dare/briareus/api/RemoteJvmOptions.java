package moe.dare.briareus.api;

import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.util.Collections.*;
import static java.util.Objects.requireNonNull;

/**
 * Options for running JVM. Consists of
 * <ul>
 *     <li>list of files to be distributed to jvm working directory</li>
 *     <li>environment variables for jvm process</li>
 *     <li>classpath which will be used to start jvm</li>
 *     <li>JVM options</li>
 *     <li>max heap size</li>
 *     <li>arguments which will be passed to main method</li>
 *     <li>context specific options</li>
 *     <li></li>
 * </ul>
 */
public class RemoteJvmOptions {
    /**
     * https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap08.html
     */
    private static final String VALID_ENVIRONMENT_VARIABLE_NAME_REGEX = "^[A-Z_]+[A-Z0-9_]*$";
    private static final Predicate<String> VALID_ENVIRONMENT_VARIABLE_NAME_PREDICATE =
            Pattern.compile(VALID_ENVIRONMENT_VARIABLE_NAME_REGEX).asPredicate();

    private final List<FileEntry> files;
    private final Map<String, String> environment;
    private final List<String> classpath;
    private final List<String> vmOptions;
    @Nullable
    private final Long maxHeapSize;
    private final String mainClass;
    private final List<String> arguments;
    private final Map<OptKey<?>, Object> opts;

    public static RemoteJvmOptionsBuilder newBuilder() {
        return new RemoteJvmOptionsBuilder();
    }

    private RemoteJvmOptions(RemoteJvmOptionsBuilder builder) {
        this.files = toUnmodifiableList(builder.files);
        this.environment = toUnmodifiableMap(builder.environmentKeys, builder.environmentValues);
        this.classpath = toUnmodifiableList(builder.classpath);
        this.vmOptions = toUnmodifiableList(builder.vmOptions);
        this.maxHeapSize = builder.maxHeapSize;
        this.mainClass = requireNonNull(builder.mainClass, "builder.mainClass");
        this.arguments = toUnmodifiableList(builder.arguments);
        this.opts = toUnmodifiableMap(builder.optKeys, builder.optValues);
    }

    public RemoteJvmOptionsBuilder toBuilder() {
        return new RemoteJvmOptionsBuilder(this);
    }

    //<editor-fold desc="Getters">
    public List<FileEntry> files() {
        return files;
    }

    public Map<String, String> environmentOverrides() {
        return environment;
    }

    public List<String> classpath() {
        return classpath;
    }

    public List<String> vmOptions() {
        return vmOptions;
    }

    public OptionalLong maxHeapSize() {
        return maxHeapSize == null ? OptionalLong.empty() : OptionalLong.of(maxHeapSize);
    }

    public String mainClass() {
        return mainClass;
    }

    public List<String> arguments() {
        return arguments;
    }

    public <T> Optional<T> getOpt(OptKey<T> optKey) {
        Object value = opts.get(optKey);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(optKey.cast(value));
    }
    //</editor-fold>

    public static class RemoteJvmOptionsBuilder {
        private final List<FileEntry> files = new ArrayList<>();
        private final List<String> environmentKeys = new ArrayList<>();
        private final List<String> environmentValues = new ArrayList<>();
        private final List<String> classpath = new ArrayList<>();
        private final List<String> vmOptions = new ArrayList<>();
        private final List<String> arguments = new ArrayList<>();
        private final List<OptKey<?>> optKeys = new ArrayList<>();
        private final List<Object> optValues = new ArrayList<>();
        @Nullable
        private Long maxHeapSize;
        @Nullable
        private String mainClass;

        private RemoteJvmOptionsBuilder() {
        }

        private RemoteJvmOptionsBuilder(RemoteJvmOptions jvmOptions) {
            addFiles(jvmOptions.files);
            addEnvironment(jvmOptions.environment);
            addClasspath(jvmOptions.classpath);
            addVmOptions(jvmOptions.vmOptions);
            addArguments(jvmOptions.arguments);
            maxHeapSize(jvmOptions.maxHeapSize);
            mainClass(jvmOptions.mainClass);
            addMapInto(jvmOptions.opts, optKeys, optValues);
        }

        public RemoteJvmOptions build() {
            if (mainClass == null) {
                throw new IllegalStateException("Main class not set");
            }
            return new RemoteJvmOptions(this);
        }

        //<editor-fold desc="Adders and setters">
        public RemoteJvmOptionsBuilder addFile(FileEntry file) {
            validateFileEntry(file);
            this.files.add(file);
            return this;
        }

        public RemoteJvmOptionsBuilder addFiles(Collection<? extends FileEntry> files) {
            requireNonNull(files, "files");
            List<FileEntry> appendableFiles = new ArrayList<>(files);
            validateFiles(appendableFiles);
            this.files.addAll(appendableFiles);
            return this;
        }

        public RemoteJvmOptionsBuilder setFiles(Collection<? extends FileEntry> files) {
            requireNonNull(files, "files");
            List<FileEntry> appendableFiles = new ArrayList<>(files);
            validateFiles(appendableFiles);
            this.files.clear();
            this.files.addAll(appendableFiles);
            return this;
        }

        public RemoteJvmOptionsBuilder addEnvironment(String key, String value) {
            validateEnvironmentEntry(key, value);
            this.environmentKeys.add(key);
            this.environmentValues.add(value);
            return this;
        }

        public RemoteJvmOptionsBuilder addEnvironment(Map<String, String> environment) {
            requireNonNull(environment, "environment");
            Map<String, String> appendEnvironment = new LinkedHashMap<>(environment);
            validateEnvironment(appendEnvironment);
            addMapInto(environment, this.environmentKeys, this.environmentValues);
            return this;
        }

        public RemoteJvmOptionsBuilder setEnvironment(Map<String, String> environment) {
            requireNonNull(environment, "environment");
            Map<String, String> appendEnvironment = new LinkedHashMap<>(environment);
            validateEnvironment(appendEnvironment);
            this.environmentKeys.clear();
            this.environmentValues.clear();
            addMapInto(environment, this.environmentKeys, this.environmentValues);
            return this;
        }

        public RemoteJvmOptionsBuilder addClasspath(String classpathEntry) {
            validateClasspathEntry(classpathEntry);
            this.classpath.add(classpathEntry);
            return this;
        }

        public RemoteJvmOptionsBuilder addClasspath(Collection<String> classpath) {
            requireNonNull(classpath, "classpath");
            List<String> appendClasspath = new ArrayList<>(classpath);
            validateClasspath(appendClasspath);
            this.classpath.addAll(appendClasspath);
            return this;
        }

        public RemoteJvmOptionsBuilder setClasspath(Collection<String> classpath) {
            requireNonNull(classpath, "classpath");
            List<String> appendClasspath = new ArrayList<>(classpath);
            validateClasspath(appendClasspath);
            this.classpath.clear();
            this.classpath.addAll(appendClasspath);
            return this;
        }

        public RemoteJvmOptionsBuilder addVmOption(String vmOption) {
            validateVmOption(vmOption);
            this.vmOptions.add(vmOption);
            return this;
        }

        public RemoteJvmOptionsBuilder addVmOptions(Collection<String> vmOptions) {
            requireNonNull(vmOptions, "vmOptions");
            List<String> appendVmOptions = new ArrayList<>(vmOptions);
            validateVmOptions(appendVmOptions);
            this.vmOptions.addAll(appendVmOptions);
            return this;
        }

        public RemoteJvmOptionsBuilder setVmOptions(Collection<String> vmOptions) {
            requireNonNull(vmOptions, "vmOptions");
            List<String> appendVmOptions = new ArrayList<>(vmOptions);
            validateVmOptions(appendVmOptions);
            this.vmOptions.clear();
            this.vmOptions.addAll(appendVmOptions);
            return this;
        }

        public RemoteJvmOptionsBuilder maxHeapSize(Long bytes) {
            validateMaxHeapSize(bytes);
            this.maxHeapSize = bytes;
            return this;
        }

        public RemoteJvmOptionsBuilder mainClass(String className) {
            validateMainClass(className);
            this.mainClass = className;
            return this;
        }

        public RemoteJvmOptionsBuilder mainClass(Class<?> clazz) {
            return mainClass(clazz.getName());
        }

        public RemoteJvmOptionsBuilder addArgument(String argument) {
            validateArgument(argument);
            this.arguments.add(argument);
            return this;
        }

        public RemoteJvmOptionsBuilder addArguments(Collection<String> arguments) {
            requireNonNull(arguments, "arguments");
            List<String> appendArguments = new ArrayList<>(arguments);
            validateArguments(appendArguments);
            this.arguments.addAll(appendArguments);
            return this;
        }

        public RemoteJvmOptionsBuilder setArguments(List<String> arguments) {
            requireNonNull(arguments, "arguments");
            List<String> appendArguments = new ArrayList<>(arguments);
            validateArguments(appendArguments);
            this.arguments.clear();
            this.arguments.addAll(appendArguments);
            return this;
        }

        public <T> RemoteJvmOptionsBuilder opt(OptKey<T> optKey, T optValue) {
            requireNonNull(optKey, "optKey");
            requireNonNull(optValue, "optValue");
            optKey.validate(optValue);
            optKeys.add(optKey);
            optValues.add(optValue);
            return this;
        }
        //</editor-fold>

        //<editor-fold desc="Validation Methods">
        private void validateFiles(List<FileEntry> files) {
            files.forEach(this::validateFileEntry);
        }

        private void validateFileEntry(FileEntry fileEntry) {
            requireNonNull(fileEntry, "file entry");
        }

        private void validateEnvironment(Map<String, String> overrides) {
            overrides.forEach(this::validateEnvironmentEntry);
        }

        private void validateEnvironmentEntry(String name, String value) {
            requireNonNull(name, "Environment variable name");
            requireNonNull(value, "Environment variable value");
            if (!VALID_ENVIRONMENT_VARIABLE_NAME_PREDICATE.test(name)) {
                throw new IllegalArgumentException("Bad environment variable name: " + name);
            }
        }

        private void validateClasspath(List<String> classpath) {
            for (String s : classpath) {
                validateClasspathEntry(s);
            }
        }

        private void validateClasspathEntry(String classpathEntry) {
            requireNonNull(classpathEntry, "Classpath entry");
        }

        private void validateVmOptions(List<String> vmOptions) {
            for (String entry : vmOptions) {
                validateVmOption(entry);
            }
        }

        private void validateVmOption(String vmOption) {
            requireNonNull(vmOption, "VM option");
        }

        private void validateArguments(List<String> arguments) {
            for (String entry : arguments) {
                validateArgument(entry);
            }
        }

        private void validateArgument(String argument) {
            requireNonNull(argument, "JVM argument");
        }

        private void validateMaxHeapSize(Long bytes) {
            if (bytes != null && bytes <= 0) {
                throw new IllegalArgumentException("Bad heap size: " + bytes);
            }
        }

        private void validateMainClass(String mainClass) {
            requireNonNull(mainClass, "main class name");
        }
        //</editor-fold>

        private static <K, V> void addMapInto(Map<K, V> map, List<K> keys, List<V> values) {
            map.forEach((k, v) -> {
                keys.add(k);
                values.add(v);
            });
        }
    }

    private static <K, V> Map<K, V> toUnmodifiableMap(List<K> keys, List<V> values) {
        if (keys == null || keys.isEmpty()) {
            return emptyMap();
        } else if (keys.size() == 1) {
            return singletonMap(keys.get(0), values.get(0));
        } else {
            Map<K, V> map = new LinkedHashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                map.put(keys.get(i), values.get(i));
            }
            return unmodifiableMap(map);
        }
    }

    private static <T> List<T> toUnmodifiableList(List<T> list) {
        if (list == null || list.isEmpty()) {
            return emptyList();
        } else if (list.size() == 1) {
            return singletonList(list.get(0));
        } else {
            return unmodifiableList(new ArrayList<>(list));
        }
    }
}
