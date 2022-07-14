package moe.dare.briareus.yarn.launch.credentials;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.common.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class UserRenewableCredentialsFactory extends CredentialsFactoryBase {
    private static final Logger log = LoggerFactory.getLogger(UserRenewableCredentialsFactory.class);
    private static final ThreadFactory THREAD_FACTORY = ThreadFactoryBuilder
            .withPrefix("User-renewable-credentials-factory-thread-").deamon(true).build();
    public static final Duration MAX_RENEW_DELAY = Duration.ofHours(11);
    public static final Duration MIN_RENEW_DELAY = Duration.ofMinutes(5);
    public static final Duration DELAY_UNTIL_MAX_LIFETIME_FOR_RENEW = Duration.ofMinutes(5);
    public static final Duration DEFAULT_MAX_LIFETIME = Duration.ofHours(6);

    private final ConcurrentMap<FsKey, UserCredentialsHolder> credentialsCache = new ConcurrentHashMap<>();
    private final Configuration conf;
    private final Supplier<UserGroupInformation> user;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService asyncExecutor;

    public static CredentialsFactory create(Supplier<UserGroupInformation> user, Configuration conf) {
        return create(user, conf, Clock.systemUTC());
    }

    static CredentialsFactory create(Supplier<UserGroupInformation> user, Configuration conf, Clock clock) {
        return new UserRenewableCredentialsFactory(user, conf, clock);
    }

    private UserRenewableCredentialsFactory(Supplier<UserGroupInformation> user, Configuration conf, Clock clock) {
        this.conf = conf;
        this.user = user;
        this.clock = clock;
        this.asyncExecutor = Executors.newCachedThreadPool(THREAD_FACTORY);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
    }

    @Override
    protected CompletableFuture<Credentials> tokens(@NotNull FsKey fsKey) {
        UserCredentialsHolder holder = credentialsCache.computeIfAbsent(fsKey, UserCredentialsHolder::new);
        return holder.getOptimistic()
                .map(CompletableFuture::completedFuture)
                .orElseGet(() -> CompletableFuture.supplyAsync(holder::get, asyncExecutor));
    }

    @Override
    public void close() {
        scheduler.shutdown();
        asyncExecutor.shutdown();
        credentialsCache.clear();
    }

    private class UserCredentialsHolder {
        private final FsKey fsKey;
        private final Lock readLock;
        private final Lock writeLock;
        private Credentials credentials;
        private Instant expiresAt;
        private Instant maxLifeTime;
        private Future<?> scheduledRenew;

        private UserCredentialsHolder(FsKey fsKey) {
            this.fsKey = fsKey;
            ReadWriteLock rwLock = new ReentrantReadWriteLock();
            readLock = rwLock.readLock();
            writeLock = rwLock.writeLock();
        }

        private Optional<Credentials> getOptimistic() {
            if (readLock.tryLock()) {
                try {
                    if (areValid()) {
                        return Optional.of(new Credentials(credentials));
                    }
                } finally {
                    readLock.unlock();
                }
            }
            return Optional.empty();
        }

        private Credentials get() {
            readLock.lock();
            try {
                if (areValid()) {
                    return new Credentials(credentials);
                }
            } finally {
                readLock.unlock();
            }
            writeLock.lock();
            try {
                return getOrCreate();
            } finally {
                writeLock.unlock();
            }
        }

        private Credentials getOrCreate() {
            if (areValid()) {
                return new Credentials(credentials);
            }
            if (scheduledRenew != null) {
                scheduledRenew.cancel(true);
                scheduledRenew = null;
            }
            if (tryRenew()) {
                return new Credentials(credentials);
            }
            createNew();
            return new Credentials(credentials);
        }

        private boolean areValid() {
            return credentials != null && expiresAt != null && clock.instant().isBefore(expiresAt);
        }

        private void createNew() {
            UserGroupInformation ugi = user.get();
            credentials = ugi.doAs((PrivilegedAction<Credentials>)() -> {
                try (FileSystem fs = FileSystem.newInstance(fsKey.toFsUri(), conf)) {
                    Credentials creds = new Credentials();
                    fs.addDelegationTokens(ugi.getUserName(), creds);
                    return creds;
                } catch (Exception e) {
                    throw new BriareusException("Can't create delegation tokens for: " + fsKey, e);
                }
            });
            boolean needRenew = credentials.getAllTokens().stream().anyMatch(t -> {
                try {
                    return t.isManaged();
                } catch (Exception e) {
                    throw new BriareusException("Can't check if token is managed", e);
                }
            });
            Instant now = clock.instant();
            maxLifeTime = credentials.getAllTokens().stream()
                    .map(UserRenewableCredentialsFactory.this::tokenMaxExpirationTime)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .min(Instant::compareTo)
                    .orElseGet(() -> now.plus(DEFAULT_MAX_LIFETIME));
            if (needRenew) {
                tryRenew();
            }
        }

        private boolean tryRenew() {
            if (credentials == null || maxLifeTime == null || maxLifeTime.isBefore(clock.instant())) {
                return false;
            }
            return user.get().doAs((PrivilegedAction<Boolean>) () -> {
                try {
                    Instant newExpiration = maxLifeTime;
                    for (Token<? extends TokenIdentifier> token : credentials.getAllTokens()) {
                        if (token.isManaged()) {
                            Instant tokenExpiration = Instant.ofEpochMilli(token.renew(conf));
                            if (tokenExpiration.compareTo(newExpiration) < 0) {
                                newExpiration = tokenExpiration;
                            }
                        }
                    }
                    expiresAt = newExpiration;
                    mayBeScheduleRenew();
                    log.debug("Renewed credentials for {}", fsKey);
                    return true;
                } catch (Exception e) {
                    log.info("Can't renew credentials", e);
                    credentials = null;
                    expiresAt = null;
                    maxLifeTime = null;
                    return false;
                }
            });
        }

        private void mayBeScheduleRenew() {
            if (maxLifeTime != null && expiresAt.isAfter(maxLifeTime.minus(DELAY_UNTIL_MAX_LIFETIME_FOR_RENEW))) {
                return;
            }
            Instant now = clock.instant();
            Duration delay = Duration.ofSeconds(now.until(expiresAt, ChronoUnit.SECONDS));
            delay = delay.dividedBy(2);
            if (delay.compareTo(MIN_RENEW_DELAY) < 0) {
                delay = MIN_RENEW_DELAY;
            } else if (delay.compareTo(MAX_RENEW_DELAY) > 0) {
                delay = MAX_RENEW_DELAY;
            }
            if (maxLifeTime != null && now.plus(delay).isAfter(maxLifeTime)) {
                return;
            }
            scheduledRenew = scheduler.schedule(this::scheduledRenew, delay.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void scheduledRenew() {
            try {
                writeLock.lockInterruptibly();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                scheduledRenew = asyncExecutor.submit(() -> {
                    try {
                        writeLock.lockInterruptibly();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    try {
                        tryRenew();
                    } finally {
                        writeLock.unlock();
                    }
                });
            } finally {
                writeLock.unlock();
            }
        }
    }
}
