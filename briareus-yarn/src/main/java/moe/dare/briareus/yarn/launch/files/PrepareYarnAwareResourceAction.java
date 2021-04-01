package moe.dare.briareus.yarn.launch.files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static java.util.Objects.requireNonNull;

final class PrepareYarnAwareResourceAction implements PrivilegedExceptionAction<LocalResource> {
    private final Configuration configuration;
    private final Path path;
    private final LocalResourceVisibility visibility;
    private final LocalResourceType type;

    PrepareYarnAwareResourceAction(Configuration configuration, Path path, LocalResourceVisibility visibility, LocalResourceType type) {
        this.configuration = requireNonNull(configuration, "configuration");
        this.path = requireNonNull(path, "path");
        this.visibility = requireNonNull(visibility, "visibility");
        this.type = requireNonNull(type, "local resource type");
    }

    @Override
    public LocalResource run() throws IOException {
        FileSystem fs = path.getFileSystem(configuration);
        Path qualifiedPath = fs.makeQualified(path);
        FileStatus fileStatus = fs.getFileStatus(qualifiedPath);
        long size = fileStatus.getLen();
        long timestamp = fileStatus.getModificationTime();
        URL resourceURL = URL.fromPath(qualifiedPath);
        return LocalResource.newInstance(resourceURL, type, visibility, size, timestamp);
    }
}
