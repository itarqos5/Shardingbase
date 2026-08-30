package dev.shardingbase.node.world;

import java.io.IOException;
import java.util.UUID;

/** Requires the proxy and local backend to authorize the exact same immutable plan. */
public final class TransactionAuthorization {
    private TransactionAuthorization() {
    }

    public static void requireMatching(
        final UUID plannedTransactionId,
        final UUID proxyAuthorization,
        final UUID backendAuthorization
    ) throws IOException {
        if (!plannedTransactionId.equals(proxyAuthorization) || !plannedTransactionId.equals(backendAuthorization)) {
            throw new IOException("Proxy and local backend did not authorize the same transaction ID");
        }
    }
}
