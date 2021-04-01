package moe.dare.briareus.yarn.launch.files;

import moe.dare.briareus.api.FileEntry;
import org.apache.hadoop.yarn.api.records.LocalResource;

import static java.util.Objects.requireNonNull;

/**
 * Class representing uploaded entry.
 */
public class UploadedEntry {
    private final FileEntry entry;
    private final LocalResource resource;

    public static UploadedEntry of(FileEntry entry, LocalResource resource) {
        return new UploadedEntry(entry, resource);
    }

    private UploadedEntry(FileEntry entry, LocalResource resource) {
        this.entry = requireNonNull(entry, "entry");
        this.resource = requireNonNull(resource, "resource");
    }

    public FileEntry entry() {
        return entry;
    }

    public LocalResource resource() {
        return resource;
    }
}
