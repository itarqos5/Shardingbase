package dev.shardingbase.protocol;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplayWindowTest {
    @Test
    void rejectsDuplicateIds() {
        final ReplayWindow window = new ReplayWindow(10, Duration.ofMinutes(1));
        final UUID id = UUID.randomUUID();

        assertTrue(window.accept(id));
        assertFalse(window.accept(id));
    }

    @Test
    void evictsOldestEntryAtCapacity() {
        final ReplayWindow window = new ReplayWindow(2, Duration.ofMinutes(1));
        final UUID first = UUID.randomUUID();
        final UUID second = UUID.randomUUID();
        final UUID third = UUID.randomUUID();

        assertTrue(window.accept(first));
        assertTrue(window.accept(second));
        assertTrue(window.accept(third));
        assertTrue(window.accept(first));
    }

    @Test
    void expiresEntriesOutsideRetention() {
        final UUID id = UUID.randomUUID();
        final MutableClock clock = new MutableClock(Instant.parse("2026-08-30T00:00:00Z"));
        final ReplayWindow window = new ReplayWindow(2, Duration.ofSeconds(1), clock);
        assertTrue(window.accept(id));
        clock.instant = Instant.parse("2026-08-30T00:00:02Z");
        assertTrue(window.accept(id));
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(final Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return this.instant;
        }
    }
}
