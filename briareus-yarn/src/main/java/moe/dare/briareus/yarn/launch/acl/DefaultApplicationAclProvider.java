package moe.dare.briareus.yarn.launch.acl;

import moe.dare.briareus.api.RemoteJvmOptions;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;

import java.util.EnumMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

enum DefaultApplicationAclProvider implements ApplicationAclProvider {
    INSTANCE;

    private static final String SUBMIT_USER_ONLY_ACCESS = " ";
    private static final Map<ApplicationAccessType, String> DEFAULT_ACLS;
    static {
        Map<ApplicationAccessType, String> acls = new EnumMap<>(ApplicationAccessType.class);
        for (ApplicationAccessType accessType : ApplicationAccessType.values()) {
            acls.put(accessType, SUBMIT_USER_ONLY_ACCESS);
        }
        DEFAULT_ACLS = unmodifiableMap(acls);
    }

    @Override
    public Map<ApplicationAccessType, String> acl(RemoteJvmOptions jvmOptions) {
        return DEFAULT_ACLS;
    }
}