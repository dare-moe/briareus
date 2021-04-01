package moe.dare.briareus.yarn.launch.auxservice;

import moe.dare.briareus.api.RemoteJvmOptions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceDataProviderTest {
    private static final RemoteJvmOptions TEST_OPTS = RemoteJvmOptions.newBuilder()
            .mainClass(ServiceDataProvider.class).build();

    @Test
    void defaultServiceDataProvider_returnsEmptyMap() {
        // given
        ServiceDataProvider provider = ServiceDataProvider.createDefault();
        // when
        Map<String, ByteBuffer> serviceData = provider.serviceData(TEST_OPTS);
        // then
        assertThat(serviceData).isEmpty();
    }
}