package moe.dare.briareus.yarn.reousrces;

import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.api.RemoteJvmOptions.RemoteJvmOptionsBuilder;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static moe.dare.briareus.yarn.reousrces.DefaultResourceFactory.INSTANCE;
import static org.assertj.core.api.Assertions.assertThat;

class DefaultResourceFactoryTest {
    private final Resource TEST_MAX_CAPABILITY = Resource.newInstance(2048, 5);

    @Test
    @DisplayName("Default cores number is 1")
    void defaultCoresTest() {
        Resource resources = INSTANCE.resources(newBuilderWithMain().build(), TEST_MAX_CAPABILITY);
        assertThat(resources.getVirtualCores()).as("Default virtual cores").isOne();
    }

    private static RemoteJvmOptionsBuilder newBuilderWithMain() {
        return RemoteJvmOptions.newBuilder().mainClass("some.class");
    }
}