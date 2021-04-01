package moe.dare.briareus.yarn.launch.files;

import moe.dare.briareus.api.FileEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * A tool responsible for uploading files to hdfs for future localizing.
 */
public interface FileUploadTool extends Closeable {
    /**
     * Creates default file upload tool which respects
     * {@link moe.dare.briareus.yarn.YarnAwareFileSource YarnAwareFileSource} and
     * {@link moe.dare.briareus.api.CacheableFileSource CacheableFileSource}.
     * Tool will use directory to upload files.
     * Implementation will not delete provided directory on close.
     * Parent of given directory must exists. If given directory exists it must be empty.
     *
     * @param user user who will upload files
     * @param conf configuration for filesystem
     * @param directory directory in which files will be uploaded.
     * @param executor executor for running io tasks
     * @return new default file upload tool
     */
    static FileUploadTool createDefault(Supplier<UserGroupInformation> user, Configuration conf, Path directory, Executor executor) {
        return DefaultFileUploadTool.create(user, conf, directory, executor);
    }

    /**
     * @param files file entries which should be uploaded to hdfs.
     * @return result of file uploading.
     */
    CompletionStage<List<UploadedEntry>> upload(List<FileEntry> files);
}
