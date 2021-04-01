package moe.dare.briareus.yarn.launch.acl;

import moe.dare.briareus.api.RemoteJvmOptions;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;

import java.util.Map;

/**
 * Service responsible for generation ACL.
 *
 * @see ApplicationAccessType
 */
public interface ApplicationAclProvider {
    /**
     * Creates default provider which.
     *
     * @return default acl provider instance
     */
    static ApplicationAclProvider createDefault() {
        return DefaultApplicationAclProvider.INSTANCE;
    }

    /**
     * Creates ACL for given jvm options.
     *
     * @param jvmOptions options for starting Sensei/Container
     * @return ACL map
     * @see ApplicationAccessType
     */
    Map<ApplicationAccessType, String> acl(RemoteJvmOptions jvmOptions);
}


