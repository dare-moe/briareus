package moe.dare.briareus.yarn.launch.credentials;

import moe.dare.briareus.api.FileEntry;
import moe.dare.briareus.api.FileSource;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.yarn.launch.files.UploadedEntry;
import moe.dare.briareus.yarn.testtools.DelegateClock;
import moe.dare.briareus.yarn.testtools.DelegateTestFs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class YarnRenewableCredentialsFactoryTest {
    private static final String RM_PRINCIPAL_NAME = "test-rm-principal";
    private final Configuration delegateConf = createConfiguration();
    private final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test-user");
    private final Instant FAR_FUTURE_MAX_DATE = Instant.ofEpochMilli(Long.MAX_VALUE);

    @Mock
    FileSystem mockFs;
    @Mock
    RemoteJvmOptions options;

    @Test
    @DisplayName("Close enough invocations should return cached tokens")
    void testCloseInvocations(@Mock(name = "test-token") Token<TokenIdentifier> token) throws IOException {
        when(token.decodeIdentifier()).thenReturn(new TestDelegationTokenIdentifier(FAR_FUTURE_MAX_DATE));
        doReturn(token).when(mockFs).getDelegationToken(RM_PRINCIPAL_NAME);
        try (Context context = new Context(mockFs)) {
            context.doCallAt(Instant.EPOCH)
                    .doCallAfter(Duration.ofMinutes(15))
                    .startValidation()
                    .allSatisfy(c -> {
                        assertThat(c.numberOfTokens()).as("number of tokens").isEqualTo(1);
                        assertThat(c.numberOfSecretKeys()).as("number of secrets").isZero();
                        assertThat(c.getAllTokens()).singleElement().isSameAs(token);
                    });
        }
        verify(mockFs).getDelegationToken(any());
    }

    @Test
    @DisplayName("Credentials considered valid for five minutes in any case")
    void testInvocations(@Mock(name = "test-token") Token<TokenIdentifier> token) throws IOException {
        when(token.decodeIdentifier()).thenReturn(new TestDelegationTokenIdentifier(Instant.EPOCH.plusSeconds(1)));
        doReturn(token).when(mockFs).getDelegationToken(RM_PRINCIPAL_NAME);
        try (Context context = new Context(mockFs)) {
            context.doCallAt(Instant.EPOCH)
                    .doCallAfter(Duration.ofMinutes(1))
                    .doCallAfter(Duration.ofMinutes(4).minusSeconds(1))
                    .startValidation()
                    .allSatisfy(c -> {
                        assertThat(c.numberOfTokens()).as("number of tokens").isEqualTo(1);
                        assertThat(c.numberOfSecretKeys()).as("number of secrets").isZero();
                        assertThat(c.getAllTokens()).singleElement().isSameAs(token);
                    });
        }
        verify(mockFs).getDelegationToken(any());
    }

    @Test
    @DisplayName("Tokens expires after 11 hours")
    void testExpirationByMaxTime(@Mock(name = "token1") Token<TokenIdentifier> token1,
                                 @Mock(name = "token2") Token<TokenIdentifier> token2) throws IOException {
        when(token1.decodeIdentifier()).thenReturn(new TestDelegationTokenIdentifier(FAR_FUTURE_MAX_DATE));
        when(token2.decodeIdentifier()).thenReturn(new TestDelegationTokenIdentifier(FAR_FUTURE_MAX_DATE));
        doReturn(token1).doReturn(token2).when(mockFs).getDelegationToken(RM_PRINCIPAL_NAME);
        try (Context context = new Context(mockFs)) {
            context.doCallAt(Instant.EPOCH)
                    .doCallAfter(Duration.ofHours(11).plusSeconds(1))
                    .startValidation()
                    .allSatisfy(c -> {
                        assertThat(c.numberOfTokens()).as("number of tokens").isEqualTo(1);
                        assertThat(c.numberOfSecretKeys()).as("number of secrets").isZero();
                    })
                    .satisfies(creds -> {
                        assertThat(creds.get(0).getAllTokens()).singleElement().isSameAs(token1);
                        assertThat(creds.get(1).getAllTokens()).singleElement().isSameAs(token2);
                    });
        }
        verify(mockFs, times(2)).getDelegationToken(any());
    }

    private static Configuration createConfiguration() {
        Configuration configuration = DelegateTestFs.createConfiguration(new Configuration());
        configuration.set(YarnConfiguration.RM_PRINCIPAL, RM_PRINCIPAL_NAME);
        return configuration;
    }

    private static class TestDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
        private TestDelegationTokenIdentifier(Instant maxDate) {
            this.setMaxDate(maxDate.toEpochMilli());
        }

        @Override
        public Text getKind() {
            return new Text("test-delegation-token");
        }
    }

    private class Context implements Closeable {
        private final String host = UUID.randomUUID().toString();
        private final DelegateClock clock = DelegateClock.create(Clock.fixed(Instant.MIN, ZoneOffset.UTC));
        private final CredentialsFactory instance =
                YarnRenewableCredentialsFactory.create(() -> ugi, delegateConf, clock);
        private final Closeable delegateFsCloseToken;
        private final List<Credentials> collectedCredentials = new ArrayList<>();

        private Context(FileSystem fs) {
            delegateFsCloseToken = DelegateTestFs.registerDelegate(host, fs);
        }

        public Context doCallAt(Instant callTime) {
            setTime(callTime);
            Credentials credentials = obtainCredentialsSync(uploadedEntry());
            collectedCredentials.add(credentials);
            return this;
        }

        public Context doCallAfter(Duration duration) {
            return doCallAt(clock.instant().plus(duration));
        }

        public ListAssert<Credentials> startValidation() {
            Set<Credentials> unique = Collections.newSetFromMap(new IdentityHashMap<>());
            unique.addAll(collectedCredentials);
            assertThat(unique).as("All credentials are not same").hasSameSizeAs(collectedCredentials);
            return assertThat(collectedCredentials);
        }

        private void setTime(Instant time) {
            clock.setInstant(time);
        }

        private Credentials obtainCredentialsSync(UploadedEntry... entries) {
            return instance.tokens(options, Arrays.asList(entries)).toCompletableFuture().join();
        }

        private UploadedEntry uploadedEntry() {
            FileSource fileSource = () -> {
                throw new AssertionError("Interaction with file source");
            };
            FileEntry fileEntry = FileEntry.copy(fileSource, "test-file");
            URL url = URL.fromURI(URI.create("delegatefs://" + host + "/myfile"));
            LocalResource resource = LocalResource.newInstance(url,
                    LocalResourceType.FILE,
                    LocalResourceVisibility.APPLICATION,
                    0, 0, null);
            return UploadedEntry.of(fileEntry, resource);
        }

        @Override
        public void close() throws IOException {
            delegateFsCloseToken.close();
            instance.close();
        }
    }
}