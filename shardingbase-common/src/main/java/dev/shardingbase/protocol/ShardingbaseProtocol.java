package dev.shardingbase.protocol;

/**
 * Constants shared by every Shardingbase process.
 */
public final class ShardingbaseProtocol {
    /** Magic number used to reject unrelated local services. */
    public static final int MAGIC = 0x53484231;

    /** Current wire protocol version. */
    public static final int VERSION = 1;

    /** Logical player synchronization channel. */
    public static final String PLAYER_SYNC_CHANNEL = "shardingbase:sync";

    /** Maximum UTF-8 bytes accepted for a control field. */
    public static final int MAX_CONTROL_FIELD_BYTES = 8_192;

    private ShardingbaseProtocol() {
    }
}
