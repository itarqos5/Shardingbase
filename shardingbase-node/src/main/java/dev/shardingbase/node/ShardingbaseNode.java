package dev.shardingbase.node;

import dev.shardingbase.protocol.ShardingbaseProtocol;

/**
 * Entry point for the backend supervisor and transaction agent.
 */
public final class ShardingbaseNode {
    private ShardingbaseNode() {
    }

    public static void main(final String[] args) {
        if (args.length == 1 && "--version".equals(args[0])) {
            System.out.println("Shardingbase Node protocol " + ShardingbaseProtocol.VERSION);
            return;
        }

        System.err.println("Shardingbase Node is not configured yet. Use --version to inspect this build.");
        System.exit(2);
    }
}
