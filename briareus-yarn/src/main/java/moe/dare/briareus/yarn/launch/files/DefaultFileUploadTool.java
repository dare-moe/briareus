package moe.dare.briareus.yarn.launch.files;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.api.CacheableFileSource;
import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.FileEntry.Mode;
import moe.dare.briareus.api.FileSource;
import moe.dare.briareus.common.utils.Maps;
import moe.dare.briareus.common.utils.Pair;
import moe.dare.briareus.yarn.YarnAwareFileSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static moe.dare.briareus.common.concurrent.CompletableFutures.failedCompletableFuture;
import static moe.dare.briareus.common.utils.Preconditions.checkState;

class DefaultFileUploadTool implements FileUploadTool {
    private static final Logger log = LoggerFactory.getLogger(DefaultFileUploadTool.class);
    private static final FsPermission DIRECTORY_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
    private static final String LOCK_FILE_NAME = ".lock";

    private static final Map<Mode, LocalResourceType> RESOURCE_TYPES = Maps.enumMapOf(
            Mode.COPY, LocalResourceType.FILE,
            Mode.UNZIP, LocalResourceType.ARCHIVE);
    private static final Map<Mode, String> EXTENSIONS = Maps.enumMapOf(
            Mode.COPY, "",
            Mode.UNZIP, ".zip");

    private final AtomicLong filesCounter = new AtomicLong();
    private final Map<Pair<Mode, CacheableFileSource>, CompletableFuture<LocalResource>> sharedFiles = new ConcurrentHashMap<>();

    private final Supplier<UserGroupInformation> user;
    private final Path directory;
    private final Configuration conf;
    private final Executor executor;
    private volatile boolean closed;

    static FileUploadTool create(Supplier<UserGroupInformation> user, Configuration conf, Path directory, Executor executor) {
        requireNonNull(user, "user");
        requireNonNull(directory, "directory");
        requireNonNull(executor, "executor");
        requireNonNull(conf, "configuration");
        prepareDirectoryAsUser(user, directory, conf);
        return new DefaultFileUploadTool(user, directory, conf, executor);
    }

    private DefaultFileUploadTool(Supplier<UserGroupInformation> user, Path directory, Configuration conf, Executor executor) {
        this.user = user;
        this.directory = directory;
        this.conf = conf;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<List<UploadedEntry>> upload(List<FileEntry> files) {
        checkState(!closed, "Upload tool closed");
        List<Pair<FileEntry, CompletableFuture<LocalResource>>> tasks = files.stream()
                .map(e -> Pair.of(e, sharedOrProcess(e)))
                .collect(toList());
        if (files.isEmpty()) {
            return completedFuture(emptyList());
        }
        if (tasks.stream().map(Pair::second).allMatch(Future::isDone)) {
            try {
                return completedFuture(blockingGet(tasks));
            } catch (Exception e) {
                return failedCompletableFuture(e);
            }
        }
        AtomicInteger counter = new AtomicInteger(tasks.size());
        CompletableFuture<List<UploadedEntry>> result = new CompletableFuture<>();
        BiConsumer<Object, Throwable> completeHandler = (unused, exception) -> {
            if (exception != null) {
                result.completeExceptionally(exception);
            } else if (counter.decrementAndGet() == 0) {
                try {
                    result.complete(blockingGet(tasks));
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            }
        };
        tasks.stream().map(Pair::second).forEach(task -> task.whenComplete(completeHandler));
        return result;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        sharedFiles.clear();
    }

    private CompletableFuture<LocalResource> sharedOrProcess(FileEntry entry) {
        FileSource source = entry.source();
        if (source instanceof CacheableFileSource) {
            Pair<Mode, CacheableFileSource> cacheKey = Pair.of(entry.mode(), (CacheableFileSource) source);
            CompletableFuture<LocalResource> future = sharedFiles.get(cacheKey);
            if (future != null && future.isCompletedExceptionally()) {
                if (sharedFiles.remove(cacheKey, future)) {
                    log.warn("Remove cached result of failed upload attempt for resource '{}' in mode '{}'.", source, entry.mode());
                }
                future = null;
            }
            if (future != null) {
                log.debug("Reusing previous upload request ({}) for file {}", future, source);
                return future;
            }
            return sharedFiles.computeIfAbsent(cacheKey, unused -> processEntry(entry));
        }
        return processEntry(entry);
    }

    private CompletableFuture<LocalResource> processEntry(FileEntry entry) {
        PrivilegedExceptionAction<LocalResource> action;
        if (entry.source() instanceof YarnAwareFileSource) {
            action = createYarnAwarePrepareAction(entry);
        } else {
            action = createCopyAction(entry);
        }
        return CompletableFuture.supplyAsync(() -> doAsUser(action), executor);
    }

    private PrepareYarnAwareResourceAction createYarnAwarePrepareAction(FileEntry entry) {
        YarnAwareFileSource source = (YarnAwareFileSource)entry.source();
        LocalResourceType type = RESOURCE_TYPES.get(entry.mode());
        Path path = source.resourcePath();
        LocalResourceVisibility visibility = source.resourceVisibility();
        return new PrepareYarnAwareResourceAction(conf, path, visibility, type);
    }

    private CopyAction createCopyAction(FileEntry entry) {
        Mode mode = entry.mode();
        LocalResourceType localResourceType = RESOURCE_TYPES.get(mode);
        String fileName = filesCounter.getAndIncrement() + "_" + entry.name() + EXTENSIONS.get(mode);
        Path filePath = new Path(directory, fileName);
        log.debug("{} will be uploaded to {}", entry, filePath);
        return new CopyAction(conf, entry.source(), filePath, localResourceType);
    }


    private LocalResource doAsUser(PrivilegedExceptionAction<LocalResource> copyAction) {
        try {
            return user.get().doAs(copyAction);
        } catch (Exception e) {
            throw new BriareusException("Can't prepare resource", e);
        }
    }

    private static List<UploadedEntry> blockingGet(List<Pair<FileEntry, CompletableFuture<LocalResource>>> tasks) {
        return tasks.stream().map(p -> UploadedEntry.of(p.first(), p.second().join())).collect(toList());
    }

    private static void prepareDirectoryAsUser(Supplier<UserGroupInformation> user, Path directory, Configuration conf) {
        UserGroupInformation currentUser = user.get();
        String userName = currentUser.getShortUserName();
        Path lockFile = new Path(directory, LOCK_FILE_NAME);
        currentUser.doAs((PrivilegedAction<Void>) () -> {
            try {
                FileSystem fileSystem = directory.getFileSystem(conf);
                if (!fileSystem.exists(directory.getParent())) {
                    throw new IllegalStateException("Parent of directory " + directory + " does not exists");
                }
                fileSystem.mkdirs(directory, DIRECTORY_PERMISSION);
                FileStatus fileStatus = fileSystem.getFileStatus(directory);
                if (!fileStatus.getOwner().equals(userName)) {
                    log.error("Directory {} owner is: {}. Current user: {}", directory, fileStatus.getOwner(), currentUser);
                    throw new IllegalArgumentException("Directory " + directory + " is owned by another user.");
                }
                if (fileSystem.listLocatedStatus(directory).hasNext()) {
                    throw new IllegalArgumentException("Directory " + directory + " not empty");
                }
                if (!fileSystem.createNewFile(lockFile)) {
                    log.error("Lock file {} creation failed", lockFile);
                    throw new IllegalArgumentException("Directory " + directory + " is locked by another process");
                }
                if (!fileStatus.getPermission().equals(DIRECTORY_PERMISSION)) {
                    log.warn("Updating directory {} permissions from {} to {}",
                            directory, fileStatus.getPermission(), DIRECTORY_PERMISSION);
                    fileSystem.setPermission(directory, DIRECTORY_PERMISSION);
                }
            } catch (IOException e) {
                throw new BriareusException("Can't prepare directory " + directory, e);
            }
            return null;
        });
        log.info("Prepared directory {} for user {}", directory, currentUser);
    }
}
