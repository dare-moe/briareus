package moe.dare.briareus.yarn.launch.credentials;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.common.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * CredentialsFactory which obtains delegation tokens for uploaded files
 */
public class YarnRenewableCredentialsFactory extends CredentialsFactoryBase {
    private static final Logger log = LoggerFactory.getLogger(YarnRenewableCredentialsFactory.class);
    private static final Duration MAX_TOKEN_VALIDITY_PERIOD = Duration.ofHours(11);
    private static final Duration MIN_TOKEN_VALIDITY_PERIOD = Duration.ofMinutes(5);
    private static final Duration TOKEN_MAX_TIME_VALIDITY_OFFSET = Duration.ofHours(1);
    private static final ThreadFactory THREAD_FACTORY = ThreadFactoryBuilder
            .withPrefix("Yarn-renewable-credentials-factory-thread-").deamon(true).build();

    private final ConcurrentMap<FsKey, CredentialsHolder> credentialsCache = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(THREAD_FACTORY);
    private final Supplier<UserGroupInformation> user;
    private final Configuration conf;
    private final String rmPrincipal;
    private final Clock clock;

    /**
     * @param user user to obtain delegation tokens.
     * @param conf yarn/hdfs configuration.
     * @return new credentials factory.
     */
    public static CredentialsFactory create(Supplier<UserGroupInformation> user, Configuration conf) {
        return create(user, conf, Clock.systemUTC());
    }

    static CredentialsFactory create(Supplier<UserGroupInformation> user, Configuration conf, Clock clock) {
        return new YarnRenewableCredentialsFactory(user, conf, clock);
    }

    private YarnRenewableCredentialsFactory(Supplier<UserGroupInformation> user, Configuration conf, Clock clock) {
        this.user = requireNonNull(user, "user");
        this.conf = requireNonNull(conf, "conf");
        this.clock = requireNonNull(clock, "clock");
        this.rmPrincipal = conf.get(YarnConfiguration.RM_PRINCIPAL);
        if (rmPrincipal == null || rmPrincipal.isEmpty()) {
            AuthenticationMethod authenticationMethod = SecurityUtil.getAuthenticationMethod(conf);
            if (authenticationMethod != AuthenticationMethod.SIMPLE) {
                log.warn("Rm principal ({}) not set for auth method {}.",
                        YarnConfiguration.RM_PRINCIPAL, authenticationMethod);
            }
        }
    }

    @Override
    protected CompletableFuture<Credentials> tokens(@NotNull FsKey fsKey) {
        CredentialsHolder holder = credentialsCache.computeIfAbsent(fsKey, CredentialsHolder::new);
        return holder.getCredentialsOptimistic()
                .map(CompletableFuture::completedFuture)
                .orElseGet(() -> CompletableFuture.supplyAsync(holder::getOrCreateCredentials, executor));
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    private class CredentialsHolder {
        private final StampedLock lock = new StampedLock();
        private final FsKey fsKey;
        private volatile Credentials credentials;
        private volatile Instant validTo = Instant.MIN;

        private CredentialsHolder(FsKey fsKey) {
            this.fsKey = fsKey;
        }

        private Optional<Credentials> getCredentialsOptimistic() {
            long optLock = lock.tryOptimisticRead();
            Credentials currentCredentials = credentials;
            if (isValid() && lock.validate(optLock)) {
                return Optional.ofNullable(currentCredentials);
            }
            return Optional.empty();
        }

        private Credentials getOrCreateCredentials() {
            Optional<Credentials> optimistic = getCredentialsOptimistic();
            if (optimistic.isPresent()) {
                return optimistic.get();
            }
            long readLock = lock.readLock();
            try {
                if (isValid()) {
                    return credentials;
                }
            } finally {
                lock.unlockRead(readLock);
            }
            long writeLock = lock.writeLock();
            try {
                if (!isValid()) {
                    createNewTokens();
                }
                return credentials;
            } finally {
                lock.unlockWrite(writeLock);
            }
        }

        private void createNewTokens() {
            Credentials newTokens = new Credentials();
            user.get().doAs((PrivilegedAction<Void>) () -> {
                try {
                    try (FileSystem fs = FileSystem.newInstance(fsKey.toFsUri(), conf)) {
                        fs.addDelegationTokens(rmPrincipal, newTokens);
                    }
                } catch (Exception e) {
                    throw new BriareusException("Can't obtain delegation tokens for " + fsKey, e);
                }
                return null;
            });
            Instant now = clock.instant();
            Instant validToLowerBound = now.plus(MIN_TOKEN_VALIDITY_PERIOD);
            Instant validToUpperBound = now.plus(MAX_TOKEN_VALIDITY_PERIOD);
            validTo = newTokens.getAllTokens().stream()
                    .map(YarnRenewableCredentialsFactory.this::tokenMaxExpirationTime)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .min(Comparator.naturalOrder())
                    .map(time -> time.minus(TOKEN_MAX_TIME_VALIDITY_OFFSET))
                    .map(time -> boundToRange(time, validToLowerBound, validToUpperBound))
                    .orElse(validToUpperBound);
            credentials = newTokens;
            log.info("Created new tokens for {}. Cached till {}", fsKey, validTo);
        }

        private Instant boundToRange(Instant value, Instant lowerBound, Instant upperBound) {
            if (value.isBefore(lowerBound)) {
                return lowerBound;
            } else if (value.isAfter(upperBound)) {
                return upperBound;
            } else {
                return value;
            }
        }

        private boolean isValid() {
            return clock.instant().isBefore(validTo);
        }
    }
}
