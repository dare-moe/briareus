package moe.dare.briareus.local;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.FileSource;
import moe.dare.briareus.common.concurrent.CancelToken;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

class FileCopyTool {
    private final Path targetDirectory;

    FileCopyTool(Path targetDirectory) {
        this.targetDirectory = requireNonNull(targetDirectory);
    }

    void copy(Collection<FileEntry> files, CancelToken token) throws IOException {
        for (FileEntry fileEntry : files) {
            copy(fileEntry, token);
        }
    }

    private void copy(FileEntry entry, CancelToken token) throws IOException {
        FileSource fileSource = entry.source();
        Path path = targetDirectory.resolve(entry.name());
        switch (entry.mode()) {
            case COPY:
                copy(fileSource, path, token);
                break;
            case UNZIP:
                unzip(fileSource, path, token);
                break;
            default:
                throw new BriareusException("Unsupported mode: " + entry.mode());
        }
    }

    private static void copy(FileSource source, Path path, CancelToken token) throws IOException {
        token.throwIfCancellationRequested();
        Optional<Path> file = source.file();
        final Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (file.isPresent()) {
            Files.copy(file.get(), path, REPLACE_EXISTING);
        } else {
            try (InputStream is = source.open()){
                Files.copy(is, path, REPLACE_EXISTING);
            }
        }
    }

    private static void unzip(FileSource source, Path dir, CancelToken token) throws IOException {
        token.throwIfCancellationRequested();
        try (InputStream is = source.open();
             ZipInputStream zipInput = new ZipInputStream(is)) {
            for (ZipEntry entry = zipInput.getNextEntry(); entry != null; entry = zipInput.getNextEntry()) {
                token.throwIfCancellationRequested();
                Path entryPath = makePath(dir, entry);
                if (entry.isDirectory()) {
                    Files.createDirectories(entryPath);
                } else {
                    Path parent = entryPath.getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                    }
                    Files.copy(zipInput, entryPath, REPLACE_EXISTING);
                }
            }
            zipInput.closeEntry();
        }
    }

    private static Path makePath(Path baseDir, ZipEntry entry) {
        Path path = baseDir.resolve(entry.getName());
        if (!baseDir.startsWith(path)) {
            throw new BriareusException("Zip entry outside target dir: " + entry.getName());
        }
        return path;
    }
}
