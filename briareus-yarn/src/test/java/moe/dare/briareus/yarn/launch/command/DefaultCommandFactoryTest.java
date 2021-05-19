package moe.dare.briareus.yarn.launch.command;

import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.api.RemoteJvmOptions.RemoteJvmOptionsBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultCommandFactoryTest {
    private static final String TEST_USER_NAME = "test-user-name";
    private static final String MAIN_CLASS = "com.example.Main";
    private final UserGroupInformation TEST_USER = UserGroupInformation.createRemoteUser(TEST_USER_NAME);

    @Test
    @DisplayName("Java arguments are base64 encoded")
    void testCommandBase64() {
        LaunchCommandFactory factory = createTestInstance(AuthenticationMethod.SIMPLE);
        LaunchOptions options = factory.createLaunchOptions(newOptions()
                .maxHeapSize(1024000L)
                .addVmOption("-Xms77777")
                .addClasspath("jar1")
                .addClasspath("jar2")
                .addClasspath("lib/*")
                .addArgument("arg1")
                .addArgument("arg2")
                .build());
        validateCommon(options);
        assertThat(options.command()).singleElement(InstanceOfAssertFactories.STRING)
                .startsWith("bash .briareus_launcher " +
                        "LVhtczc3Nzc3 " + //-Xms77777
                        "LWNw " + //-cp
                        "amFyMTpqYXIyOmxpYi8q " + //jar1:jar2:lib/*
                        "LVhteDEwMjQwMDA= " + //-Xmx1024000
                        "Y29tLmV4YW1wbGUuTWFpbg== " + //com.example.Main
                        "YXJnMQ== " + //arg1
                        "YXJnMg== "); //arg2
    }

    @Test
    @DisplayName("With simple auth HADOOP_USER_NAME should be set")
    void testSimpleAuth() {
        LaunchCommandFactory factory = createTestInstance(AuthenticationMethod.SIMPLE);
        LaunchOptions options = factory.createLaunchOptions(newOptions().build());
        validateCommon(options);
        assertThat(options.environment()).hasSize(1).containsEntry("HADOOP_USER_NAME", TEST_USER_NAME);
    }

    @Test
    @DisplayName("With kerberos auth HADOOP_USER_NAME should not be set")
    void testKerberosAuth() {
        LaunchCommandFactory factory = createTestInstance(AuthenticationMethod.KERBEROS);
        LaunchOptions options = factory.createLaunchOptions(newOptions().build());
        validateCommon(options);
        assertThat(options.environment()).isEmpty();
    }

    @Test
    @DisplayName("When simple auth provided HADOOP_USER_NAME is used")
    void testSimpleAuthWithHadoopUserNameProvided() {
        LaunchCommandFactory factory = createTestInstance(AuthenticationMethod.SIMPLE);
        RemoteJvmOptions jvmOptions = newOptions().addEnvironment("HADOOP_USER_NAME", "foo").build();
        LaunchOptions options = factory.createLaunchOptions(jvmOptions);
        validateCommon(options);
        assertThat(options.environment()).hasSize(1).containsEntry("HADOOP_USER_NAME", "foo");
    }

    private void validateCommon(LaunchOptions options) {
        assertThat(options.launcherFiles()).as("launcher files")
                .singleElement()
                .satisfies(fe -> {
                    assertThat(fe.name()).as("launch script remote name").isEqualTo(".briareus_launcher");
                    assertThat(fe.mode()).as("launch script mode").isEqualTo(FileEntry.Mode.COPY);
                });
        assertThat(options.command()).as("command")
                .singleElement(InstanceOfAssertFactories.STRING).startsWith("bash .briareus_launcher ");
    }

    private RemoteJvmOptionsBuilder newOptions() {
        return RemoteJvmOptions.newBuilder().mainClass(MAIN_CLASS);
    }

    private LaunchCommandFactory createTestInstance(AuthenticationMethod authMethod) {
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(authMethod, configuration);
        return DefaultCommandFactory.create(TEST_USER, configuration);
    }
}