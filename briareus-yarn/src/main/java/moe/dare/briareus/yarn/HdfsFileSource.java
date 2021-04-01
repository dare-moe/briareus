package moe.dare.briareus.yarn;

import moe.dare.briareus.api.CacheableFileSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class HdfsFileSource implements YarnAwareFileSource, CacheableFileSource {
    private final Configuration configuration;
    private final Path path;
    private final LocalResourceVisibility localResourceVisibility;

    public static HdfsFileSource publicScopeSource(Path path, Configuration configuration) {
        return new HdfsFileSource(configuration, path, LocalResourceVisibility.PUBLIC);
    }

    public static HdfsFileSource applicationScopeSource(Path path, Configuration configuration) {
        return new HdfsFileSource(configuration, path, LocalResourceVisibility.APPLICATION);
    }

    public static HdfsFileSource privateScopeSource(Path path, Configuration configuration) {
        return new HdfsFileSource(configuration, path, LocalResourceVisibility.PRIVATE);
    }

    public static HdfsFileSource scoped(Path path, Configuration configuration, LocalResourceVisibility visibility) {
        return new HdfsFileSource(configuration, path, visibility);
    }

    private HdfsFileSource(Configuration configuration, Path path, LocalResourceVisibility localResourceVisibility) {
        this.configuration = requireNonNull(configuration, "configuration");
        this.localResourceVisibility = requireNonNull(localResourceVisibility, "localResourceVisibility");
        this.path = requireNonNull(path, "path");
    }

    @Override
    public Path resourcePath() {
        return path;
    }

    @Override
    public LocalResourceVisibility resourceVisibility() {
        return localResourceVisibility;
    }

    @Override
    public InputStream open() throws IOException {
        return path.getFileSystem(configuration).open(path);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HdfsFileSource that = (HdfsFileSource) o;
        return configuration.equals(that.configuration) &&
                path.equals(that.path) &&
                localResourceVisibility == that.localResourceVisibility;
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration, path, localResourceVisibility);
    }

    @Override
    public String toString() {
        return "HdfsFileSource{" +
                "configuration=" + configuration +
                ", path=" + path +
                ", localResourceVisibility=" + localResourceVisibility +
                '}';
    }
}
