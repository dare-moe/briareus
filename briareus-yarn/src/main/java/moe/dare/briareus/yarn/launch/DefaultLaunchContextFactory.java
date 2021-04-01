package moe.dare.briareus.yarn.launch;

import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.yarn.launch.acl.ApplicationAclProvider;
import moe.dare.briareus.yarn.launch.auxservice.ServiceDataProvider;
import moe.dare.briareus.yarn.launch.command.LaunchCommandFactory;
import moe.dare.briareus.yarn.launch.command.LaunchOptions;
import moe.dare.briareus.yarn.launch.credentials.CredentialsFactory;
import moe.dare.briareus.yarn.launch.files.FileUploadTool;
import moe.dare.briareus.yarn.launch.files.UploadedEntry;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * An implementation of {@link LaunchContextFactory} which works as facade.
 *
 * @see FileUploadTool
 * @see LaunchCommandFactory
 * @see CredentialsFactory
 * @see ServiceDataProvider
 * @see ApplicationAclProvider
 */
public class DefaultLaunchContextFactory implements LaunchContextFactory {
    private final FileUploadTool fileUploadTool;
    private final LaunchCommandFactory launchCommandFactory;
    private final CredentialsFactory credentialsFactory;
    private final ServiceDataProvider serviceDataProvider;
    private final ApplicationAclProvider aclProvider;

    /**
     * @return new builder for DefaultLaunchContextFactory
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    private DefaultLaunchContextFactory(Builder builder) {
        this.fileUploadTool = requireNonNull(builder.fileUploadTool, "FileUploadTool");
        this.credentialsFactory = requireNonNull(builder.credentialsFactory, "CredentialsFactory");
        this.launchCommandFactory = requireNonNull(builder.launchCommandFactory, "LaunchCommandFactory");
        this.serviceDataProvider = ofNullable(builder.serviceDataProvider).orElseGet(ServiceDataProvider::createDefault);
        this.aclProvider = ofNullable(builder.aclProvider).orElseGet(ApplicationAclProvider::createDefault);
    }

    @Override
    public CompletionStage<ContainerLaunchContext> create(RemoteJvmOptions jvmOptions) {
        verifyOptions(jvmOptions);
        LaunchOptions launchOptions = launchCommandFactory.createLaunchOptions(jvmOptions);
        Map<String, ByteBuffer> serviceData = serviceDataProvider.serviceData(jvmOptions);
        Map<ApplicationAccessType, String> acls = aclProvider.acl(jvmOptions);
        CompletionStage<List<UploadedEntry>> uploadedEntriesFuture = uploadFiles(jvmOptions, launchOptions);
        CompletionStage<Credentials> credentialsFuture = uploadedEntriesFuture.thenCompose(entries ->
                credentialsFactory.tokens(jvmOptions, entries));
        return credentialsFuture.thenApply(credentials -> ContainerLaunchContext.newInstance(
                mergeUploadedList(uploadedEntriesFuture.toCompletableFuture().join()),
                launchOptions.environment(),
                launchOptions.command(),
                serviceData,
                tokenStorageBytes(credentials),
                acls));
    }

    private void verifyOptions(RemoteJvmOptions options) {
        requireNonNull(options);
    }

    private CompletionStage<List<UploadedEntry>> uploadFiles(RemoteJvmOptions jvmOptions, LaunchOptions launchOptions) {
        List<FileEntry> entries = new ArrayList<>(jvmOptions.files().size() + launchOptions.launcherFiles().size());
        entries.addAll(jvmOptions.files());
        entries.addAll(launchOptions.launcherFiles());
        return fileUploadTool.upload(entries);
    }

    private static Map<String, LocalResource> mergeUploadedList(List<UploadedEntry> uploadedFiles) {
        return uploadedFiles.stream().collect(Collectors.toMap(e -> e.entry().name(), UploadedEntry::resource));
    }

    private static ByteBuffer tokenStorageBytes(Credentials credentials) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            credentials.writeTokenStorageToStream(new DataOutputStream(baos));
            return ByteBuffer.wrap(baos.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("Can't write token storage to DataOutputStream backed by ByteArrayOutputStream", e);
        }
    }

    /**
     * Builder for {@link DefaultLaunchContextFactory}.
     * <br>Required properties:
     * <ul>
     *     <li>fileUploadTool</li>
     *     <li>credentialsFactory</li>
     *     <li>launchCommandFactory</li>
     * </ul>
     * <br>Optional properties:
     * <ul>
     *     <li>serviceDataProvider</li>
     *     <li>aclProvider</li>
     * </ul>
     *
     */
    public static class Builder {
        private FileUploadTool fileUploadTool;
        private CredentialsFactory credentialsFactory;
        private LaunchCommandFactory launchCommandFactory;
        private ServiceDataProvider serviceDataProvider;
        private ApplicationAclProvider aclProvider;

        private Builder() {
        }

        /**
         * Required parameter.
         *
         * @param fileUploadTool FileUploadTool instance
         * @return this builder for chaining
         */
        public Builder fileUploadTool(FileUploadTool fileUploadTool) {
            this.fileUploadTool = requireNonNull(fileUploadTool, "fileUploadTool");
            return this;
        }

        /**
         * Required parameter.
         *
         * @param credentialsFactory CredentialsFactory instance
         * @return this builder for chaining
         */
        public Builder credentialsFactory(CredentialsFactory credentialsFactory) {
            this.credentialsFactory = requireNonNull(credentialsFactory, "credentialsFactory");
            return this;
        }

        /**
         * Required parameter.
         *
         * @param launchCommandFactory LaunchCommandFactory instance
         * @return this builder for chaining
         */
        public Builder launchCommandFactory(LaunchCommandFactory launchCommandFactory) {
            this.launchCommandFactory = requireNonNull(launchCommandFactory, "launchCommandFactory");
            return this;
        }

        /**
         * Optional parameter
         *
         * @param serviceDataProvider ServiceDataProvider instance
         * @return this builder for chaining
         */
        public Builder serviceDataProvider(ServiceDataProvider serviceDataProvider) {
            this.serviceDataProvider = requireNonNull(serviceDataProvider, "serviceDataProvider");
            return this;
        }

        /**
         * Optional parameter
         *
         * @param aclProvider ApplicationAclProvider instance
         * @return this builder for chaining
         */
        public Builder aclProvider(ApplicationAclProvider aclProvider) {
            this.aclProvider = requireNonNull(aclProvider, "aclProvider");
            return this;
        }

        /**
         * @return new LaunchContextFactory
         * @throws IllegalStateException if some required parameter is not set.
         */
        public LaunchContextFactory build() {
            try {
                return new DefaultLaunchContextFactory(this);
            } catch (NullPointerException npe) {
                throw new IllegalStateException("Required parameter not set", npe);
            }
        }
    }
}
