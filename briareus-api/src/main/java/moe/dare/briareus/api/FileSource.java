package moe.dare.briareus.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Source for files distributed to JVM.
 * @see CacheableFileSource
 */
public interface FileSource {
    /**
     * Open stream for underlying resource.
     *
     * @return input stream
     * @throws IOException if opening fails
     */
    InputStream open() throws IOException;

    /**
     * Return path to file if this FileSource represents file on the filesystem.
     *
     * @return path to underlying file or empty optional.
     */
    default Optional<Path> file() {
        return Optional.empty();
    }

    /**
     * The requirement for equals. Two file sources may be equal if
     * and only if their input stream always produces same bytes without external modification of underlying resource.
     * E.g. file sources pointing to the same file may be considered equal.
     */
    boolean equals(Object other);
}
