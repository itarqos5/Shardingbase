package dev.shardingbase.protocol;

import java.io.IOException;

/** Malformed, oversized, corrupt, or incompatible protocol data. */
public final class ProtocolException extends IOException {
    public ProtocolException(final String message) {
        super(message);
    }

    public ProtocolException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
