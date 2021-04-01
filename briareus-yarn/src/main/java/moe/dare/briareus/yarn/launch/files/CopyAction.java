package moe.dare.briareus.yarn.launch.files;

import moe.dare.briareus.api.FileSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

final class CopyAction implements PrivilegedExceptionAction<LocalResource> {
    private static final FsPermission FILE_PERMISSION = new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE);
    private static final EnumSet<CreateFlag> CREATE_FLAGS = EnumSet.of(CreateFlag.CREATE);
    private static final LocalResourceVisibility RESOURCE_VISIBILITY = LocalResourceVisibility.APPLICATION;

    private final Configuration configuration;
    private final FileSource source;
    private final Path target;
    private final LocalResourceType type;

    CopyAction(Configuration configuration, FileSource source, Path target, LocalResourceType type) {
        this.configuration = requireNonNull(configuration, "configuration");
        this.source = requireNonNull(source, "file source");
        this.target = requireNonNull(target, "target path");
        this.type = requireNonNull(type, "local resource type");
    }

    @Override
    public LocalResource run() throws IOException {
        FileSystem fs = target.getFileSystem(configuration);
        Path targetQualified = fs.makeQualified(target);
        URL targetUrl = URL.fromPath(targetQualified);
        int bufferSize = fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT);
        short replication = fs.getDefaultReplication(targetQualified);
        long blockSize = fs.getDefaultBlockSize(targetQualified);
        long fileReadSize;
        try (OutputStream out = fs.create(targetQualified, FILE_PERMISSION, CREATE_FLAGS, bufferSize, replication, blockSize, null);
             InputStream in = source.open()) {
            fileReadSize = copy(in, out, bufferSize);
        }
        long timestamp = fs.getFileStatus(targetQualified).getModificationTime();
        return LocalResource.newInstance(targetUrl, type, RESOURCE_VISIBILITY, fileReadSize, timestamp);
    }

    private static long copy(InputStream in, OutputStream out, int bufferSize) throws IOException {
        long nRead = 0;
        byte[] buffer = new byte[bufferSize];
        int n;
        while ((n = in.read(buffer)) > 0) {
            out.write(buffer, 0, n);
            nRead += n;
        }
        return nRead;
    }
}
