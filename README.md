# Briareus project

![Maven Central][shield-maven-version] ![GitHub][shield-license]

Welcome to Briareus. An open source library set dedicated to simplify starting Java applications 
in clustered environment. Specifically under Apache YARN resource manager.

Please note that until reaching version 1.x API is considered unstable. 

Documentation is available at [briareus.dare.moe](https://briareus.dare.moe)

Please note that `briareus-common` module is internal dependency and is not mean to be used by end users.

## Yarn quickstart 

Add dependency to your project:

Groovy users:
```groovy
implementation "moe.dare.briareus:briareus-yarn:0.1.0"
```

Maven adepts:
```xml
<dependency>
    <groupId>moe.dare.briareus</groupId>
    <artifactId>briareus-yarn</artifactId>
    <version>0.1.0</version>
</dependency>
```

> :warning: the following code is just a short snippet to bootstrap your briareus experience.

For submitting new applications to YARN create `BriareusYarnShodanContext`

```java
org.apache.hadoop.conf.Configuration conf = ...;
org.apache.hadoop.fs.Path hdfsUploadDir = new Path("hdfs:///tmp/" + UUID.randomUUID());
UserGroupInformation user = UserGroupInformation.getCurrentUser();
Supplier<UserGroupInformation> userSupplier = () -> user;
ExecutorService ioPool = Executors.newCachedThreadPool();

try (FileUploadTool tool = FileUploadTool.createDefault(userSupplier, conf, hdfsUploadDir, ioPool);
     CredentialsFactory credentialsFactory = YarnRenewableCredentialsFactory.create(userSupplier, conf);
     LaunchCommandFactory launchCommandFactory = LaunchCommandFactory.createDefault(user, conf);
     BriareusYarnShodanContext context = BriareusYarnShodanContextBuilder.newBuilder()
                 .launchContextFactory(DefaultLaunchContextFactory.newBuilder()
                         .fileUploadTool(tool)
                         .credentialsFactory(credentialsFactory)
                         .launchCommandFactory(launchCommandFactory)
                         .build())
                 .user(userSupplier)
                 .configuration(conf)
                 .build()) {
        RemoteJvmOptions senseiOptions = RemoteJvmOptions.newBuilder()
                .maxHeapSize(128L * 1024 * 1024)
                .addFiles(filesToDistribute)
                .setClasspath(remoteClasspath)
                .addVmOption("-Dorg.slf4j.simpleLogger.defaultLogLevel=debug")
                .addEnvironment("ANSWER_TO_LIFE", "42")
                .mainClass(senseiMainClass)
                .addArgument("--foo")
                .addArgument("bar")
                .opt(CommonOpts.YARN_CONTAINER_CORES, 2)
                .opt(ShodanOpts.YARN_APPLICATION_NAME, "My awesome app!")
                .build();
        YarnSenseiJvmProcess proc = context.start(senseiOptions).toCompletableFuture().join();
        proc.onExit().toCompletableFuture().join();
        System.out.println("Final status: " + proc.completionStatus());
    } finally {
        hdfsUploadDir.getFileSystem(conf).delete(hdfsUploadDir, true);
    }
}
```

For starting containers use `BriareusYarnSenseiContext`:
```java
org.apache.hadoop.conf.Configuration conf = ...;
org.apache.hadoop.fs.Path hdfsUploadDir = new Path("hdfs:///tmp/" + UUID.randomUUID());
UserGroupInformation user = UserGroupInformation.getCurrentUser();
Supplier<UserGroupInformation> userSupplier = () -> user;
ExecutorService ioPool = Executors.newCachedThreadPool();
Runnable shutdownRequestHandler = () -> {
    Thread thread = new Thread(() -> System.exit(1));
    thread.setDaemon(true);
    thread.start();
};

try (FileUploadTool fileUploadTool = FileUploadTool.createDefault(userSupplier, conf, hdfsUploadDir, ioPool);
     CredentialsFactory credentialsFactory = YarnRenewableCredentialsFactory.create(userSupplier, conf);
     LaunchCommandFactory launchCommandFactory = LaunchCommandFactory.createDefault(user, conf);
     BriareusYarnSenseiContext ctxt = BriareusYarnSenseiContextBuilder.newBuilder()
             .launchContextFactory(DefaultLaunchContextFactory.newBuilder()
                     .fileUploadTool(fileUploadTool)
                     .credentialsFactory(credentialsFactory)
                     .launchCommandFactory(launchCommandFactory)
                     .build())
             .shutdownRequestHandler(shutdownRequestHandler)
             .configuration(conf)
             .build()) {
    try {
        RemoteJvmOptions containerOptions = RemoteJvmOptions.newBuilder()
                .maxHeapSize(128L * 1024 * 1024)
                .addFiles(filesToDistribute)
                .setClasspath(remoteClasspath)
                .addVmOption("-Dorg.slf4j.simpleLogger.defaultLogLevel=debug")
                .mainClass(containerMainClass)
                .addArgument("--baz")
                .addArgument("bazingo")
                .build();
        RemoteJvmProcess process = ctxt.start(containerOptions).toCompletableFuture().join();
        process.onExit().toCompletableFuture().join();
        OptionalInt exitCode = process.exitCode();
        System.out.println("Container exit code: " + exitCode);
        if (!exitCode.isPresent() || exitCode.getAsInt() != 0) {
            ctxt.setFinalStatus(ApplicationStatus.failed("Bad exit code " + exitCode));
        }
    } catch (Exception e) {
        ctxt.setFinalStatus(ApplicationStatus.failed("Exception " + e.getMessage()));
    }
} finally {
    hdfsUploadDir.getFileSystem(conf).delete(hdfsUploadDir, true);
}
```

[shield-maven-version]: <https://img.shields.io/maven-central/v/moe.dare.briareus/briareus-api>
[shield-license]: <https://img.shields.io/github/license/dare-moe/briareus>