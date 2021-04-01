package moe.dare.briareus.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static moe.dare.briareus.common.utils.Preconditions.checkState;

public class DefaultWorkDirectoryFactory implements WorkDirectoryFactory {
    private final AtomicInteger counter = new AtomicInteger();
    private final Path basePath;
    private volatile boolean closed;

    public static DefaultWorkDirectoryFactory create(Path basePath) {
        return new DefaultWorkDirectoryFactory(basePath);
    }

    private DefaultWorkDirectoryFactory(Path basePath) {
        this.basePath = requireNonNull(basePath, "base jvm directories path");
        try {
            Files.createDirectories(basePath);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't create base directory " + basePath, e);
        }
    }

    @Override
    public Path createDirectory() throws IOException {
        checkState(!closed);
        String dirName = "jvm" + counter.getAndIncrement();
        Path jvmDir = basePath.resolve(dirName);
        return Files.createDirectory(jvmDir);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public String toString() {
        return "DefaultWorkingDirectoryFactory{basePath=" + basePath + '}';
    }
}
