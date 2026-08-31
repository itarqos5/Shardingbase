package dev.shardingbase.velocity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VelocityWorldTransactionCoordinatorTest {
    @Test
    void reservesTwentyPercentWithoutOverflow() {
        assertEquals(120L, VelocityWorldTransactionCoordinator.safetyMargin(100L));
        assertEquals(Long.MAX_VALUE, VelocityWorldTransactionCoordinator.safetyMargin(Long.MAX_VALUE));
    }
}
