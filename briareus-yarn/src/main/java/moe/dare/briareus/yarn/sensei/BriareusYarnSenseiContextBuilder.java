package moe.dare.briareus.yarn.sensei;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.yarn.launch.LaunchContextFactory;
import moe.dare.briareus.yarn.reousrces.ResourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static moe.dare.briareus.common.utils.Preconditions.checkState;


/**
 * Builder for BriareusYarnSenseiContext.
 * <br>Required parameters:
 * <ul>
 *     <li>configuration</li>
 *     <li>launchContextFactory</li>
 * </ul>
 * <br>Optional parameters:
 * <ul>
 *     <li>user</li>
 *     <li>resourceFactory</li>
 *     <li>shutdownRequestHandler</li>
 *     <li>host</li>
 *     <li>port</li>
 *     <li>trackingUrl</li>
 *     <li>stopContainersOnClose</li>
 * </ul>
 */
public class BriareusYarnSenseiContextBuilder {
    private static final Logger log = LoggerFactory.getLogger(BriareusYarnSenseiContextBuilder.class);
    private static final int NO_RPC_PORT = -1;

    private UserGroupInformation user;
    private LaunchContextFactory launchContextFactory;
    private ResourceFactory resourceFactory;
    private Runnable shutdownRequestHandler;
    private Configuration configuration;
    private String host;
    private int port = NO_RPC_PORT;
    private String trackingUrl;
    private boolean nmClientCleanupContainers = true;

    public static BriareusYarnSenseiContextBuilder newBuilder() {
        return new BriareusYarnSenseiContextBuilder();
    }

    private BriareusYarnSenseiContextBuilder() {
    }

    /**
     * Required property.
     *
     * @param configuration yarn configuration
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder configuration(Configuration configuration) {
        this.configuration = requireNonNull(configuration, "configuration");
        return this;
    }

    /**
     * Required property.
     *
     * @param launchContextFactory LaunchContextFactory instance
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder launchContextFactory(LaunchContextFactory launchContextFactory) {
        this.launchContextFactory = requireNonNull(launchContextFactory, "launchContextFactory");
        return this;
    }

    /**
     * Optional property.
     * Sets user who poses {@code AMRMToken} for communication with Resource Manager.
     *
     * @param user yarn user
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder user(UserGroupInformation user) {
        this.user = requireNonNull(user, "user");
        return this;
    }

    /**
     * Optional property.
     *
     * @param resourceFactory ResourceFactory instance
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder resourceFactory(ResourceFactory resourceFactory) {
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory");
        return this;
    }

    /**
     * Optional property. Sets the runnable which will be executed when Resource Manager asks JVM to stop.
     *
     * @param shutdownRequestHandler runnable to be executed on shutdown request.
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder shutdownRequestHandler(Runnable shutdownRequestHandler) {
        this.shutdownRequestHandler = requireNonNull(shutdownRequestHandler, "shutdownRequestHandler");
        return this;
    }

    /**
     * Optional property.
     *
     * @param host hostname to be registered in YARN resource manager.
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder hostname(String host) {
        this.host = requireNonNull(host, "host");
        return this;
    }

    /**
     * Optional property.
     *
     * @param port rpc port to be registered in YARN resource manager. Greater or equal to -1.
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder port(int port) {
        if (port < -1) {
            throw new IllegalArgumentException("Port can't be less then -1, provided: " + port);
        }
        this.port = port;
        return this;
    }

    /**
     * Optional property.
     *
     * @param trackingUrl tracking url to be registered in YARN resource manager.
     * @return this instance for chaining
     */
    public BriareusYarnSenseiContextBuilder trackingUrl(String trackingUrl) {
        this.trackingUrl = requireNonNull(trackingUrl, "trackingUrl");
        return this;
    }

    /**
     * Optional property.
     * If true will <b>proactively</b> stop started containers on context close.
     *
     * @return this instance for chaining.
     */
    public BriareusYarnSenseiContextBuilder stopContainersOnClose(boolean value) {
        this.nmClientCleanupContainers = value;
        return this;
    }

    public BriareusYarnSenseiContext build() {
        checkState(configuration != null, "configuration not set");
        checkState(launchContextFactory != null, "launch context factory not set");
        ResourceFactory resourceFactoryOrDefault = ofNullable(resourceFactory).orElseGet(ResourceFactory::createDefault);
        UserGroupInformation userOrDefault = ofNullable(user).orElseGet(this::createDefaultUser);
        String hostOrDefault = ofNullable(host).orElseGet(this::getDefaultHost);
        Runnable shutdownRequestHandlerOrDefault = ofNullable(shutdownRequestHandler)
                .orElse(DefaultShutdownRequestHandler.INSTANCE);
        BriareusYarnSenseiContextImpl context = new BriareusYarnSenseiContextImpl(
                userOrDefault,
                launchContextFactory,
                resourceFactoryOrDefault,
                shutdownRequestHandlerOrDefault);
        context.startContext(configuration, hostOrDefault, port, trackingUrl, nmClientCleanupContainers);
        return context;
    }

    protected Optional<String> getEnvironmentVariable(String name) {
        return ofNullable(System.getenv(name));
    }

    private UserGroupInformation createDefaultUser() {
        log.info("Creating default sensei user.");
        try {
            String userName = getEnvironmentVariable(ApplicationConstants.Environment.USER.key()).orElseThrow(() ->
                    new IllegalStateException("No variable named " + ApplicationConstants.Environment.USER.key() + " in environment"));
            Credentials credentials = getDefaultSenseiCredentials();
            UserGroupInformation senseiDefaultUser = UserGroupInformation.createRemoteUser(userName);
            senseiDefaultUser.addCredentials(credentials);
            log.info("Created sensei user {}", senseiDefaultUser);
            return senseiDefaultUser;
        } catch (Exception e) {
            throw new BriareusException("Can't create sensei user", e);
        }
    }

    private Credentials getDefaultSenseiCredentials() {
        Optional<String> tokenFile = getEnvironmentVariable(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
        if (!tokenFile.isPresent()) {
            String msg = "Environment variable " + UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION + " not set";
            throw new IllegalStateException(msg);
        }
        Credentials credentials = new Credentials();
        Path tokenFilePath = Paths.get(tokenFile.get());
        try (InputStream inputStream = Files.newInputStream(tokenFilePath);
             DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            credentials.readTokenStorageStream(dataInputStream);
        } catch (Exception e) {
            throw new IllegalStateException("Can't read credentials from token file", e);
        }
        credentials.getAllTokens().removeIf(token -> !AMRMTokenIdentifier.KIND_NAME.equals(token.getKind()));
        List<Text> secretKeys = new ArrayList<>(credentials.getAllSecretKeys());
        secretKeys.forEach(credentials::removeSecretKey);
        if (credentials.numberOfTokens() <= 0) {
            throw new IllegalStateException("No tokens of kind " + AMRMTokenIdentifier.KIND_NAME + " in token file");
        }
        return credentials;
    }

    private String getDefaultHost() {
        log.info("Host not configured. Getting default hostname");
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            throw new BriareusException("Can't get current host name", e);
        }
    }
}
