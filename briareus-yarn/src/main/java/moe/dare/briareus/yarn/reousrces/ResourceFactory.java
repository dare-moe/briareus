package moe.dare.briareus.yarn.reousrces;

import moe.dare.briareus.api.RemoteJvmOptions;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Determines resources required to run JVM in YARN.
 */
public interface ResourceFactory {
    /**
     * Creates default factory instance which respects
     * {@link moe.dare.briareus.yarn.CommonOpts#YARN_CONTAINER_CORES YARN_CONTAINER_CORES}
     * and {@link moe.dare.briareus.yarn.CommonOpts#YARN_CONTAINER_MEMORY_MB YARN_CONTAINER_MEMORY_MB}
     * options.
     *
     * @return default resource factory.
     */
    static ResourceFactory createDefault() {
        return DefaultResourceFactory.INSTANCE;
    }

    /**
     * @param options remote jvm options
     * @param maximumCapability maximum cluster capability
     * @return resources required to start container
     */
    Resource resources(RemoteJvmOptions options, Resource maximumCapability);
}
