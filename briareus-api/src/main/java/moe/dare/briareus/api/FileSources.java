package moe.dare.briareus.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * <p>This class contains static methods for creating file sources.</p>
 * <p>Unless other is stated returned instances implements {@link CacheableFileSource}</p>
 */
public class FileSources {
    private FileSources() {
    }

    /**
     * Creates files source pointing to classpath source.
     *
     * @param classLoader classloader which will be used to access resource
     * @param resourceName classloader's resource name
     * @return new file source
     * @throws IllegalArgumentException if given classloader does not contain resource with given name
     */
    public static FileSource classpathSource(ClassLoader classLoader, String resourceName) {
        if (classLoader.getResource(resourceName) == null) {
            throw new IllegalArgumentException("No resource named " + resourceName + " in " + classLoader);
        }
        return new ClasspathSource(classLoader, resourceName);
    }

    /**
     * @param path path to resource
     * @return new file source pointing to given file.
     * @throws IllegalArgumentException if given path does not exists or its existence cannot be determined.
     */
    public static FileSource fileSource(Path path) {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Path " + path + " dose not exists");
        }
        return new FilePathSource(path);
    }

    private static final class FilePathSource implements CacheableFileSource {
        private final Path path;

        private FilePathSource(Path path) {
            this.path = requireNonNull(path, "path");
        }

        @Override
        public InputStream open() throws IOException {
            return Files.newInputStream(path);
        }

        @Override
        public Optional<Path> file() {
            return Optional.of(path);
        }


        @Override
        public int hashCode() {
            return path.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FilePathSource that = (FilePathSource) o;
            return path.equals(that.path);
        }
    }

    private static final class ClasspathSource implements CacheableFileSource {
        private final ClassLoader classLoader;
        private final String resourceName;

        private ClasspathSource(ClassLoader classLoader, String resourceName) {
            this.classLoader = requireNonNull(classLoader, "Classloader");
            this.resourceName = requireNonNull(resourceName, "resource name");
        }

        @Override
        public InputStream open() throws IOException {
            InputStream stream = classLoader.getResourceAsStream(this.resourceName);
            if (stream == null) {
                throw new IOException("No classpath resource [" + resourceName + "] in classloader " + classLoader);
            }
            return stream;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClasspathSource that = (ClasspathSource) o;
            if (!classLoader.equals(that.classLoader)) return false;
            return resourceName.equals(that.resourceName);
        }

        @Override
        public int hashCode() {
            int result = classLoader.hashCode();
            result = 31 * result + resourceName.hashCode();
            return result;
        }
    }
}
