package moe.dare.briareus.yarntest;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.FileSources;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.api.RemoteJvmProcess;
import moe.dare.briareus.yarn.CommonOpts;
import moe.dare.briareus.yarn.HdfsFileSource;
import moe.dare.briareus.yarn.launch.DefaultLaunchContextFactory;
import moe.dare.briareus.yarn.launch.command.LaunchCommandFactory;
import moe.dare.briareus.yarn.launch.credentials.UserRenewableCredentialsFactory;
import moe.dare.briareus.yarn.launch.credentials.YarnRenewableCredentialsFactory;
import moe.dare.briareus.yarn.launch.files.FileUploadTool;
import moe.dare.briareus.yarn.sensei.ApplicationStatus;
import moe.dare.briareus.yarn.sensei.BriareusYarnSenseiContext;
import moe.dare.briareus.yarn.sensei.BriareusYarnSenseiContextBuilder;
import moe.dare.briareus.yarn.shodan.BriareusYarnShodanContext;
import moe.dare.briareus.yarn.shodan.BriareusYarnShodanContextBuilder;
import moe.dare.briareus.yarn.shodan.ShodanOpts;
import moe.dare.briareus.yarn.shodan.YarnSenseiJvmProcess;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;


public class Test {
    private static final String SENSEI = "sensei";
    private static final String CONTAINER_FOO = "container_foo";
    private static final String CONTAINER_BAR = "container_bar";
    private static final java.nio.file.Path JAR_PATH = Paths.get(Test.class.getProtectionDomain().getCodeSource().getLocation().getPath());
    private static final String JAR_NAME = "Test.jar";
    private static final Configuration conf;

    static {
        conf = new Configuration();
        Stream.of("core-site.xml", "hdfs-site.xml", "yarn-site.xml")
                .map(x -> {
                    try {
                        return Paths.get("/etc/hadoop/conf/" + x).toUri().toURL();
                    } catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .forEach(conf::addResource);
    }

    private static final FileEntry JAR = FileEntry.copy(FileSources.fileSource(JAR_PATH), JAR_NAME);
    private static final FileEntry JRE = FileEntry.unzip(HdfsFileSource.publicScopeSource(new Path("hdfs:///tmp/adopt_openjdk_11.zip"), conf), "JRE");
    private static final Path dir = new Path("hdfs:///tmp/" + UUID.randomUUID());
    private static final ExecutorService pool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("upload-pool-%d").setDaemon(true).build());

    public static void main(String[] args) throws Exception {
        System.out.println("This is env");
        System.getenv().forEach((k, v) -> System.out.println(k + " = " + v));
        System.out.println("===========");
        System.out.println("java.version=" + System.getProperty("java.version"));
        System.out.println("java.home=" + System.getProperty("java.home"));

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.main(new String[0]);
        if (args.length == 0) {
            submit(conf);
            dir.getFileSystem(conf).delete(dir, true);
        } else if (args[0].equals(SENSEI)) {
            sensei(conf);
            dir.getFileSystem(conf).delete(dir, true);
        } else if (args[0].equals(CONTAINER_FOO)) {
            Instant start = Instant.now();
            while (Duration.between(start, Instant.now()).compareTo(Duration.ofDays(8)) < 0) {
                System.out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + " HELLO FROM CONTAINER FOO!");
                Thread.sleep(30 * 60 * 1000);
            }
        } else if (args[0].equals(CONTAINER_BAR)) {
            System.out.println("HELLO FROM CONTAINER BAR!");
        }
    }

    public static void submit(Configuration conf) throws Exception {
        UserGroupInformation user = UserGroupInformation.getCurrentUser();
        Supplier<UserGroupInformation> supplier = () -> user;

        try (FileUploadTool tool = FileUploadTool.createDefault(supplier, conf, dir, pool);
             BriareusYarnShodanContext context = BriareusYarnShodanContextBuilder.newBuilder()
                     .launchContextFactory(DefaultLaunchContextFactory.newBuilder()
                             .fileUploadTool(tool)
                             .credentialsFactory(YarnRenewableCredentialsFactory.create(supplier, conf))
                             .launchCommandFactory(LaunchCommandFactory.createDefault(user, conf))
                             .build())
                     .user(supplier)
                     .configuration(conf)
                     .build()) {
            final RemoteJvmOptions options = RemoteJvmOptions.newBuilder()
                    .maxHeapSize(128L * 1024 * 1024)
                    .addFiles(Arrays.asList(JAR, JRE))
                    .addVmOption("-Dorg.slf4j.simpleLogger.defaultLogLevel=debug")
                    //.addEnvironment("JAVA_HOME", "./JRE/")
                    .setClasspath(singletonList(JAR_NAME))
                    .mainClass(Test.class.getName())
                    .addArgument(SENSEI)
                    .opt(CommonOpts.YARN_CONTAINER_CORES, 2)
                    .opt(ShodanOpts.YARN_APPLICATION_NAME, "My awesome app!")
                    .build();
            CompletableFuture<YarnSenseiJvmProcess> p = context.start(options).toCompletableFuture();
            YarnSenseiJvmProcess proc = p.thenCompose(YarnSenseiJvmProcess::onExit).thenApply(YarnSenseiJvmProcess.class::cast).join();
            System.out.println("Final status: " + proc.completionStatus());
        }
    }

    private static void sensei(Configuration conf) throws Exception {
        System.out.println("Sensei!");
        UserGroupInformation curuser = UserGroupInformation.getCurrentUser();
        Supplier<UserGroupInformation> user = () -> curuser;
        try (FileUploadTool fileUploadTool = FileUploadTool.createDefault(user, conf, dir, pool);
             BriareusYarnSenseiContext ctxt = BriareusYarnSenseiContextBuilder.newBuilder()
                     .launchContextFactory(DefaultLaunchContextFactory.newBuilder()
                             .fileUploadTool(fileUploadTool)
                             .credentialsFactory(UserRenewableCredentialsFactory.create(user, conf))
                             .launchCommandFactory(LaunchCommandFactory.createDefault(curuser, conf))
                             .build())
                     .shutdownRequestHandler(() -> {
                         final Thread thread = new Thread(() -> System.exit(1));
                         thread.setDaemon(true);
                         thread.start();
                     })
                     .configuration(conf)
                     .build()) {
            try {
                final RemoteJvmOptions fooOpts = RemoteJvmOptions.newBuilder()
                        .maxHeapSize(128L * 1024 * 1024)
                        .addFiles(Arrays.asList(JAR, JRE))
                        //.addEnvironment("JAVA_HOME", "./JRE/")
                        .setClasspath(singletonList(JAR_NAME))
                        .mainClass(Test.class.getName())
                        .setArguments(singletonList(CONTAINER_FOO))
                        .build();
                final RemoteJvmOptions barOpts = fooOpts.toBuilder()
                        .setArguments(singletonList(CONTAINER_BAR))
                        .build();
                RemoteJvmProcess process = ctxt.start(fooOpts).toCompletableFuture().join();
                CompletableFuture<RemoteJvmProcess> fooEnd = process.onExit().toCompletableFuture();
                while (!fooEnd.isDone()) {
                    ctxt.start(barOpts);
                    Thread.sleep(20 * 60 * 1000);
                }
                OptionalInt exitCode = process.exitCode();
                System.out.println("Container exit code: " + exitCode);
                if (!exitCode.isPresent() || exitCode.getAsInt() != 0) {
                    ctxt.setFinalStatus(ApplicationStatus.failed("Bad exit code " + exitCode));
                }
            } catch (Exception e) {
                ctxt.setFinalStatus(ApplicationStatus.failed("Exception " + e.getMessage()));
            }
        }
    }
}
