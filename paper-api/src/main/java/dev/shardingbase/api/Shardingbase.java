package dev.shardingbase.api;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jetbrains.annotations.ApiStatus;

/**
 * Access to the Shardingbase service supplied by a Shardingbase server.
 */
public final class Shardingbase {
    private static final ShardingbaseService UNAVAILABLE = new UnavailableService();
    private static volatile ShardingbaseService service = UNAVAILABLE;

    private Shardingbase() {
    }

    /**
     * Gets the service for the running server.
     *
     * @return active service, or a disabled service when not running on Shardingbase
     */
    public static ShardingbaseService service() {
        return service;
    }

    /**
     * Installs the server-owned service.
     *
     * @param newService service implementation
     */
    @ApiStatus.Internal
    public static void installService(final ShardingbaseService newService) {
        service = Objects.requireNonNull(newService, "newService");
    }

    /** Resets the service during server shutdown. */
    @ApiStatus.Internal
    public static void clearService() {
        service = UNAVAILABLE;
    }

    private static final class UnavailableService implements ShardingbaseService {
        private static final ServerIdentity IDENTITY = new ServerIdentity("unavailable", "unavailable");

        @Override
        public ServerIdentity identity() {
            return IDENTITY;
        }

        @Override
        public FeatureState featureState() {
            return FeatureState.DISABLED;
        }

        @Override
        public String statusDetail() {
            return "Shardingbase is not installed on this server";
        }

        @Override
        public CompletionStage<ReloadResult> reload() {
            return CompletableFuture.completedFuture(new ReloadResult(false, this.statusDetail()));
        }
    }
}
