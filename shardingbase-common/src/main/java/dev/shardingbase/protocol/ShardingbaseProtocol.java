package dev.shardingbase.protocol;

/**
 * Constants shared by every Shardingbase process.
 */
public final class ShardingbaseProtocol {
    /** Current wire protocol version. */
    public static final int VERSION = 1;

    /** Logical player synchronization channel. */
    public static final String PLAYER_SYNC_CHANNEL = "shardingbase:sync";

    private ShardingbaseProtocol() {
    }
}
