---
title: Api guide
sidebar_label: Api
---

## RemoteJvmOptions

Specification of JVM instance. Includes
 
* List of files to be distributed to JVM working directory
* Environment variables
* Classpath
* Main class name
* JVM options
* Maximum heap size
* Arguments for application itself
* Context specific options

The only required property is main class. Everything else is up to you.
For building specification use a fluent builder.

### Files

To start your java application you almost always need to distribute some files. At least your application artifacts.
`FileEntry` describes file you want to be available on remote machine.
Each entry is described by file source, remote filename and mode.

:::caution

Remote JVM instances MUST not delete or modify distributed files.

:::

`FileSource` is a common interface for accessing files from the local machine. It may be local file, classpath resource
or anything you want.

:::tip

When implementing your own file source consider implement `CacheableFileSource` marker annotation.

:::

For better portability remote file names should be portable according to [POSIX.1-2017][posix-portable-filename]
and the last character should not be `.` (period). I.e. permitted characters are:

* English uppercase and lowercase letters
* Digits from 0 to 9
* Period `.`
* Underscore `_`
* Hyphen-minus `-`

Finally, there are two modes:

* **Copy** just copies provided file source to remote JVM's working directory.
 
* **Unzip** unarchives provided file into directory.

### Environment variables.

Sure you can set environment variables for remote JVM.
Once again for better portability only uppercase english letters and underscore are permitted as variable names.

:::note

Please avoid special characters in variable names.

:::

## Maximum heap size

Determines the Xmx parameter for your JVM instance.

## Remote process.

Small abstraction representing JVM instance. It's interface if fairly simple
and inspired by `java.lang.Process` in Java 9. 

```java
public interface RemoteJvmProcess {
    void destroy();
    void destroyForcibly();
    boolean isAlive();
    OptionalInt exitCode();
    CompletionStage<RemoteJvmProcess> onExit();
}
```

## Briareus context.

Core part responsible for starting JVM instances.

```java
public interface BriareusContext<T extends RemoteJvmProcess> extends AutoCloseable {
    CompletionStage<T> start(RemoteJvmOptions options);
}
```

Just pass JVM spec and the magic begins :smile:

[posix-portable-filename]: <https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_281>