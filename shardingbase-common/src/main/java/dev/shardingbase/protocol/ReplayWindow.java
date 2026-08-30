package dev.shardingbase.protocol;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Bounded time window for correlation-ID replay rejection. */
public final class ReplayWindow {
    private final int maximumEntries;
    private final Duration retention;
    private final Clock clock;
    private final Map<UUID, Instant> seen = new LinkedHashMap<>();

    public ReplayWindow(final int maximumEntries, final Duration retention) {
        this(maximumEntries, retention, Clock.systemUTC());
    }

    ReplayWindow(final int maximumEntries, final Duration retention, final Clock clock) {
        if (maximumEntries < 1) {
            throw new IllegalArgumentException("maximumEntries must be positive");
        }
        if (retention.isNegative() || retention.isZero()) {
            throw new IllegalArgumentException("retention must be positive");
        }
        this.maximumEntries = maximumEntries;
        this.retention = retention;
        this.clock = clock;
    }

    /**
     * Records an ID if it has not appeared inside the active window.
     *
     * @param correlationId frame correlation ID
     * @return true for a new ID, false for a replay
     */
    public synchronized boolean accept(final UUID correlationId) {
        final Instant now = this.clock.instant();
        final Instant cutoff = now.minus(this.retention);
        final Iterator<Map.Entry<UUID, Instant>> iterator = this.seen.entrySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getValue().isAfter(cutoff)) {
                break;
            }
            iterator.remove();
        }
        if (this.seen.containsKey(correlationId)) {
            return false;
        }
        while (this.seen.size() >= this.maximumEntries) {
            final Iterator<UUID> oldest = this.seen.keySet().iterator();
            oldest.next();
            oldest.remove();
        }
        this.seen.put(correlationId, now);
        return true;
    }
}
