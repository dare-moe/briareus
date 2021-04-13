package moe.dare.briareus.yarn.testtools;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static java.util.Objects.requireNonNull;

public class DelegateClock extends Clock {
    private final ZoneId zoneId;
    private volatile Clock delegate;

    public static DelegateClock create(Clock delegate) {
        return new DelegateClock(delegate, ZoneOffset.UTC);
    }

    public DelegateClock(Clock delegate, ZoneId zoneId) {
        this.delegate = requireNonNull(delegate, "delegate");
        this.zoneId = requireNonNull(zoneId, "zoneId");
    }

    public void setDelegate(Clock delegate) {
        this.delegate = requireNonNull(delegate, "delegate");
    }

    public void setInstant(Instant instant) {
        requireNonNull(instant, "instant");
        this.delegate = Clock.fixed(instant, zoneId);
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new DelegateClock(delegate, zone);
    }

    @Override
    public Instant instant() {
        return delegate.instant();
    }
}
