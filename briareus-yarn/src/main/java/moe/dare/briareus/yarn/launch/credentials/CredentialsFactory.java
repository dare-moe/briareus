package moe.dare.briareus.yarn.launch.credentials;

import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.yarn.launch.files.UploadedEntry;
import org.apache.hadoop.security.Credentials;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * Factory responsible for obtaining credentials for JVM's container.
 * It's recommended that implementations provide delegation tokens for uploaded entries.
 * @see YarnRenewableCredentialsFactory
 */
public interface CredentialsFactory extends Closeable {
    /**
     * @param options JVM options.
     * @param uploadedEntries entries uploaded by {@link moe.dare.briareus.yarn.launch.files.FileUploadTool}
     * @return completion stage with credentials
     */
    CompletionStage<Credentials> tokens(RemoteJvmOptions options, Collection<UploadedEntry> uploadedEntries);

    @Override
    default void close() {
    }
}
