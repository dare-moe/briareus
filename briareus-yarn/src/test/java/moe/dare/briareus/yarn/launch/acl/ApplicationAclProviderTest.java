package moe.dare.briareus.yarn.launch.acl;

import moe.dare.briareus.api.RemoteJvmOptions;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ApplicationAclProviderTest {
    private static final RemoteJvmOptions TEST_OPTS = RemoteJvmOptions.newBuilder()
            .mainClass(ApplicationAclProvider.class)
            .build();

    @Test
    void defaultProvider_returnsStrictMap() {
        // given
        ApplicationAclProvider aclProvider = ApplicationAclProvider.createDefault();
        // when
        Map<ApplicationAccessType, String> aclMap = aclProvider.acl(TEST_OPTS);
        // then
        assertThat(aclMap).as("Checkins ACL map is strict")
                .containsOnlyKeys(ApplicationAccessType.values())
                .allSatisfy((k, v) ->
                    assertThat(v).as("Acl '%s' value", k).isEqualTo(" ")
                );
    }
}