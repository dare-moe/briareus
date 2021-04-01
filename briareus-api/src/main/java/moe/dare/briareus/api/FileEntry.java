package moe.dare.briareus.api;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * <p>Description of file which should be placed to JVM working directory.</p>
 *
 * <p>For better compatibility names of files <b>must</b> be portable according to
 * POSIX.1-2017 standard and not ends with '.' (period).</p>
 * I.e. allowed name characters are
 * <ul>
 *     <li>English uppercase and lowercase letters</li>
 *     <li>Digits 0-9</li>
 *     <li>Period '.'</li>
 *     <li>Underscore '_'</li>
 *     <li>hyphen-minus '-'</li>
 * </ul>
 */
public final class FileEntry {
    /**
     * Portable charset: https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_282
     */
    private static final String PORTABLE_FILENAME_REGEX = "^[a-zA-Z0-9_.-]*[a-zA-Z0-9_-]$";
    private static final Predicate<String> VALID_PATH_PREDICATE = Pattern.compile(PORTABLE_FILENAME_REGEX).asPredicate();

    private final String name;
    private final FileSource fileSource;
    private final Mode mode;

    /**
     * @param fileSource file source for this entry.
     * @param name       portable file name
     * @return new copy file entry
     * @throws IllegalArgumentException if bad name is provided.
     */
    public static FileEntry copy(FileSource fileSource, String name) {
        return new FileEntry(fileSource, name, Mode.COPY);
    }

    /**
     * @param fileSource fire source for this entry pointing to valid zip archive
     * @param name       portable directory name
     * @return new unzip file entry
     */
    public static FileEntry unzip(FileSource fileSource, String name) {
        return new FileEntry(fileSource, name, Mode.UNZIP);
    }

    /**
     * @return file source for this entry
     */
    public FileSource source() {
        return fileSource;
    }

    /**
     * @return name of file/folder of entry in jvm working directory
     */
    public String name() {
        return name;
    }

    /**
     * @return distribution mode. Copy or unzip.
     */
    public Mode mode() {
        return mode;
    }

    private FileEntry(FileSource fileSource, String name, Mode mode) {
        this.fileSource = requireNonNull(fileSource, "file source");
        this.name = requireNonNull(name, "file entry name");
        this.mode = requireNonNull(mode, "file entry mode");
        if (!VALID_PATH_PREDICATE.test(name)) {
            throw new IllegalArgumentException("Bad file entry name: " + name);
        }
    }

    public enum Mode {
        COPY,
        UNZIP
    }
}
