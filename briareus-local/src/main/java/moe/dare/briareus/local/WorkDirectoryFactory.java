package moe.dare.briareus.local;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public interface WorkDirectoryFactory extends Closeable {
    Path createDirectory() throws IOException;
}
