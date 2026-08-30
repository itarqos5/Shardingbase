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
        if (args.length == 1 && "--shardingbase-node-version".equals(args[0])) {
            System.out.println("Shardingbase Node protocol " + ShardingbaseProtocol.VERSION);
            return;
        }

        try {
            final BackendExtractor.ExtractionResult extraction = BackendExtractor.extractEmbeddedBackend();
            final String action = extraction.updated() ? "exported" : "already current";
            System.out.println("Shardingbase backend " + action + " at " + extraction.path());

            if (args.length == 1 && "--shardingbase-extract-only".equals(args[0])) {
                System.out.println("Shardingbase backend extraction completed without starting the server.");
                return;
            }

            System.out.println("Starting the Shardingbase backend as a supervised process.");
            final int exitCode = BackendProcess.run(extraction.path(), args);
            System.out.println("Shardingbase backend exited with code " + exitCode + '.');
            if (exitCode != 0) {
                System.exit(exitCode);
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while supervising the Shardingbase backend.");
            System.exit(130);
        } catch (final IOException | URISyntaxException exception) {
            System.err.println("Unable to start the embedded Shardingbase backend: " + exception.getMessage());
            exception.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
