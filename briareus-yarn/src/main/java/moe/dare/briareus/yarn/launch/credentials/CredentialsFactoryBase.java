package moe.dare.briareus.yarn.launch.credentials;

import moe.dare.briareus.api.BriareusException;
import moe.dare.briareus.api.RemoteJvmOptions;
import moe.dare.briareus.yarn.launch.files.UploadedEntry;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

abstract class CredentialsFactoryBase implements CredentialsFactory {
    private static final Logger log = LoggerFactory.getLogger(CredentialsFactoryBase.class);

    private final AsyncCallsCache<FsKey, Credentials> callsCache = new AsyncCallsCache<>();

    @Override
    public CompletionStage<Credentials> tokens(RemoteJvmOptions options, Collection<UploadedEntry> entries) {
        Set<FsKey> keys = entries.stream().map(FsKey::keyFor).collect(Collectors.toSet());
        List<CompletableFuture<Credentials>> allCredentials = new ArrayList<>(keys.size());
        try {
            for (FsKey key : keys) {
                allCredentials.add(callsCache.callOrCache(key, this::tokens));
            }
            return combine(allCredentials);
        } catch (Exception e) {
            throw new BriareusException("Can't acquire delegation tokens", e);
        }
    }

    protected abstract CompletableFuture<Credentials> tokens(@NotNull FsKey fsKey);

    private static CompletableFuture<Credentials> combine(List<CompletableFuture<Credentials>> credentials) {
        if (credentials.isEmpty()) {
            return completedFuture(new Credentials());
        } else if (credentials.size() == 1) {
            return credentials.get(0).thenApply(Credentials::new);
        }
        return CompletableFuture.allOf(credentials.toArray(new CompletableFuture<?>[0]))
                .thenApply(any -> {
                    Credentials combined = new Credentials();
                    credentials.stream().map(CompletableFuture::join).forEach(combined::addAll);
                    return combined;
                });
    }

    protected final Optional<Instant> tokenMaxExpirationTime(Token<? extends TokenIdentifier> token) {
        try {
            TokenIdentifier identifier = token.decodeIdentifier();
            if (identifier instanceof AbstractDelegationTokenIdentifier) {
                long timestamp = ((AbstractDelegationTokenIdentifier) identifier).getMaxDate();
                return Optional.of(Instant.ofEpochMilli(timestamp));
            }
            log.debug("Delegation token identified for token of kind {} is not instance of {} but {}",
                    token.getKind(), AbstractDelegationTokenIdentifier.class, token.getClass());
        } catch (Exception e) {
            log.warn("Can't decode identifier for token of kind: {}", token.getKind(), e);
        }
        return Optional.empty();
    }
}
