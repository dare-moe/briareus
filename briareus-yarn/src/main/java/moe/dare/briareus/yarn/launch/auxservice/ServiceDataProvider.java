package moe.dare.briareus.yarn.launch.auxservice;

import moe.dare.briareus.api.RemoteJvmOptions;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * Creates service data for <b>AuxiliaryService</b>
 *
 * @see org.apache.hadoop.yarn.api.records.ContainerLaunchContext
 */
public interface ServiceDataProvider {
    /**
     * @return default service data provider which provides no service data.
     */
    static ServiceDataProvider createDefault() {
        return any -> Collections.emptyMap();
    }

    /**
     * @param options JVM options
     * @return service data for ContainerLaunchContext
     */
    Map<String, ByteBuffer> serviceData(RemoteJvmOptions options);
}
