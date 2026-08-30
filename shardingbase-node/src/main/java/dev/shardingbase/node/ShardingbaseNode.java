package dev.shardingbase.node;

import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Entry point for the backend supervisor and transaction agent.
 */
public final class ShardingbaseNode {
    private ShardingbaseNode() {
    }

    public static void main(final String[] args) {
        try {
            final BackendExtractor.ExtractionResult extraction = BackendExtractor.extractEmbeddedBackend();
            final String action = extraction.updated() ? "exported" : "already current";
            System.out.println("Shardingbase backend " + action + " at " + extraction.path());

            if (args.length == 1 && "--version".equals(args[0])) {
                System.out.println("Shardingbase Node protocol " + ShardingbaseProtocol.VERSION);
                return;
            }

            System.out.println("Shardingbase manager is ready; the backend was not started.");
        } catch (final IOException | URISyntaxException exception) {
            System.err.println("Unable to export the embedded Shardingbase backend: " + exception.getMessage());
            exception.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
