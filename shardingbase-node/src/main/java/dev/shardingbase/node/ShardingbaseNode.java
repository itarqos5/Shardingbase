package dev.shardingbase.node;

import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

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
            final Path managerDirectory = BackendExtractor.managerDirectory();
            final BackendExtractor.ExtractionResult extraction =
                BackendExtractor.extractEmbeddedBackend(managerDirectory);
            final String action = extraction.updated() ? "exported" : "already current";
            System.out.println("Shardingbase backend " + action + " at " + extraction.path());

            if (args.length == 1 && "--shardingbase-extract-only".equals(args[0])) {
                System.out.println("Shardingbase backend extraction completed without starting the server.");
                return;
            }

            try (
                ProxyValidationClient proxyClient = new ProxyValidationClient();
                NodeFileTransferHandler fileTransfers = new NodeFileTransferHandler(proxyClient, stagingRoot());
                LocalBackendController controller = LocalBackendController.start(proxyClient);
                BackendProcess backend = BackendProcess.launch(
                    extraction.path(),
                    managerDirectory,
                    args,
                    controller.childEnvironment()
                );
                NodeWorldTransactionController transactions =
                    NodeWorldTransactionController.start(proxyClient, controller, backend)
            ) {
                System.out.println("Starting the Shardingbase backend as a supervised process.");
                final int exitCode = backend.waitForExit();
                System.out.println("Shardingbase backend exited with code " + exitCode + '.');
                if (exitCode != 0) {
                    System.exit(exitCode);
                }
                System.out.println("Shardingbase supervisor is stopping because the backend stopped.");
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

    private static Path stagingRoot() {
        final String configured = System.getenv().getOrDefault(
            NodeFileTransferHandler.STAGING_ROOT_ENVIRONMENT,
            "shardingbase-staging"
        );
        final Path path = Path.of(configured);
        return (path.isAbsolute() ? path : Path.of(System.getProperty("user.dir")).resolve(path))
            .toAbsolutePath()
            .normalize();
    }
}
