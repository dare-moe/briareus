package moe.dare.briareus.yarn.shodan;

import moe.dare.briareus.yarn.launch.LaunchContextFactory;
import moe.dare.briareus.yarn.reousrces.ResourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;
import static moe.dare.briareus.common.utils.Preconditions.checkState;

/**
 * Builder for BriareusYarnShodanContextBuilder.
 * <br>Required parameters:
 * <ul>
 *     <li>user</li>
 *     <li>configuration</li>
 *     <li>launchContextFactory</li>
 * </ul>
 * <br>Optional parameters:
 * <ul>
 *     <li>resourceFactory</li>
 *     <li>yarnClientFactory</li>
 * </ul>
 */
public class BriareusYarnShodanContextBuilder {
    private Supplier<UserGroupInformation> user;
    private LaunchContextFactory launchContextFactory;
    private ResourceFactory resourceFactory;
    private Configuration configuration;
    private YarnClientFactory yarnClientFactory;

    public static BriareusYarnShodanContextBuilder newBuilder() {
        return new BriareusYarnShodanContextBuilder();
    }

    private BriareusYarnShodanContextBuilder() {
    }

    /**
     * Required property.
     *
     * @param user user to submit applications
     * @return this instance for chaining
     */
    public BriareusYarnShodanContextBuilder user(Supplier<UserGroupInformation> user) {
        this.user = Objects.requireNonNull(user, "user");
        return this;
    }

    /**
     * Required property.
     *
     * @param configuration yarn configuration
     * @return this instance for chaining
     */
    public BriareusYarnShodanContextBuilder configuration(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        return this;
    }

    /**
     * Required property.
     *
     * @param launchContextFactory LaunchContextFactory instance
     * @return this instance for chaining
     */
    public BriareusYarnShodanContextBuilder launchContextFactory(LaunchContextFactory launchContextFactory) {
        this.launchContextFactory = Objects.requireNonNull(launchContextFactory, "launchContextFactory");
        return this;
    }

    /**
     * Optional property.
     *
     * @param resourceFactory ResourceFactory instance
     * @return this instance for chaining
     */
    public BriareusYarnShodanContextBuilder resourceFactory(ResourceFactory resourceFactory) {
        this.resourceFactory = Objects.requireNonNull(resourceFactory, "resourceFactory");
        return this;
    }

    /**
     * Optional property.
     *
     * @param yarnClientFactory YarnClientFactory instance
     * @return this instance for chaining
     */
    public BriareusYarnShodanContextBuilder yarnClientFactory(YarnClientFactory yarnClientFactory) {
        this.yarnClientFactory = Objects.requireNonNull(yarnClientFactory, "yarnClientFactory");
        return this;
    }

    public BriareusYarnShodanContext build() {
        checkState(user != null, "user not set");
        checkState(configuration != null, "configuration not set");
        checkState(launchContextFactory != null, "launch context factory not set");
        ResourceFactory resourceFactoryOrDefault = ofNullable(resourceFactory).orElseGet(ResourceFactory::createDefault);
        UgiYarnClient client = new UgiYarnClient(user, yarnClientFactory);
        client.start(configuration);
        try {
            return new BriareusYarnShodanContextImpl(client, launchContextFactory, resourceFactoryOrDefault);
        } catch (Exception e) {
            try {
                client.stop();
            } catch (Exception stopException) {
                e.addSuppressed(stopException);
            }
            throw e;
        }
    }
}
