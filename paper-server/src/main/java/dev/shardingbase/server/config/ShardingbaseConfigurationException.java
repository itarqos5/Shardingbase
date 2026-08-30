package dev.shardingbase.server.config;

/** Fatal identity configuration failure which must stop server startup. */
public final class ShardingbaseConfigurationException extends Exception {
    public ShardingbaseConfigurationException(final String message) {
        super(message);
    }

    public ShardingbaseConfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
