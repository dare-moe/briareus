---
title: Overview
slug: /
---

Briareus is a library set dedicated to start JVM instances in different clustered environments. 
It's divided into separate artifacts for specific environments and common api module.
Common api give us ability to writing code reusable across implementations agnostic to cluster details.
Requires Java 8 or later.

Three main concepts are:
* Remote JVM options - describes specification for starting your application.
* Remote JVM process - similar to `java.lang.Process` gives you ability to start or stop processes. 
* Context - blackbox that does the dark magic.

Please refer to [Api guide](guides/api.md) for more details.

And starting new JVM instance is really simple.

```java
// Obtain context
BriareusContext<? extends RemoteJvmProcess> context = ...
// Create specification
FileSource jarSource = FileSources.fileSource(Paths.get("local/path/my-awesome-locacl.jar");
String remoteJarName = "my-awesome.jar";
FileEntry file = FileEntry.copy(jarSource, remoteJarName);

RemoteJvmOptions spec = RemoteJvmOptions.newBuilder()
  .maxHeapSize(128L * 1024 * 1024)
  .addFile(file)
  .addVmOption("-Dorg.slf4j.simpleLogger.defaultLogLevel=debug")
  .addEnvironment("MY_ENV_VAR", "Briareus is great")
  .addClasspath(remoteJarName)
  .mainClass("com.example.Main")
  .addArgument("--my-argument")
  .build();
// run and wait for completion
RemoteJvmProcess process = context.start().toCompletableFuture().join();
process.onExit().toCompletableFuture().join();
``` 

Why so weird declaration for context? Well, different context may expose implementations of RemoteJvmProcess with
additional functionality. Thanks to Java [use site variance][wiki-use-site-variance] it's almost impossible to
build nice api exposing methods that returns ```CompletionStage```.
As a rule of thumb use PECS mantra or specific context interface. 

[wiki-use-site-variance]: <https://en.wikipedia.org/wiki/Covariance_and_contravariance_(computer_science)#Use-site_variance_annotations_(wildcards)>
