package dev.shardingbase.velocity;

import dev.shardingbase.protocol.WorldTransactionCodec;
import dev.shardingbase.protocol.WorldTransactionCodec.Manifest;
import dev.shardingbase.protocol.WorldTransactionCodec.Operation;
import dev.shardingbase.protocol.WorldTransactionCodec.Outcome;
import dev.shardingbase.protocol.WorldTransactionCodec.Request;
import dev.shardingbase.protocol.WorldTransactionCodec.Response;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

/** SQLite-journaled authority that preflights immutable cut plans against both live nodes. */
final class VelocityWorldTransactionCoordinator implements AutoCloseable {
    private static final Duration NODE_TIMEOUT = Duration.ofSeconds(10);

    private final WorldPlannerStore store;
    private final BackendRegistry registry;
    private final ControlServer control;
    private final Logger logger;
    private final byte[] signingKey;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(task -> Thread.ofPlatform()
        .daemon(true)
        .name("Shardingbase Transaction Authority")
        .unstarted(task));
    private final AtomicBoolean closed = new AtomicBoolean();

    VelocityWorldTransactionCoordinator(
        final VelocityConfiguration configuration,
        final WorldPlannerStore store,
        final BackendRegistry registry,
        final ControlServer control,
        final Logger logger
    ) throws IOException {
        this.store = store;
        this.registry = registry;
        this.control = control;
        this.logger = logger;
        try {
            this.signingKey = Base64.getUrlDecoder().decode(configuration.transactionSigningKey());
        } catch (final IllegalArgumentException exception) {
            throw new IOException("Velocity transaction signing key is invalid", exception);
        }
        if (this.signingKey.length < 32) {
            throw new IOException("Velocity transaction signing key is too short");
        }
        for (final WorldPlannerStore.TransactionPlan plan : store.transactionsIn("PLANNED", "PREFLIGHTING")) {
            this.submit(plan.transactionId());
        }
    }

    void submit(final UUID transactionId) {
        if (this.closed.get()) {
            return;
        }
        try {
            this.worker.execute(() -> this.preflight(transactionId));
        } catch (final RuntimeException exception) {
            if (!this.closed.get()) {
                this.logger.warn("Unable to schedule Shardingbase transaction {}", transactionId, exception);
            }
        }
    }

    private void preflight(final UUID transactionId) {
        String state = null;
        try {
            WorldPlannerStore.TransactionPlan plan = this.store.transaction(transactionId)
                .orElseThrow(() -> new IOException("World transaction plan does not exist"));
            state = plan.state();
            if ("PLANNED".equals(state)) {
                this.store.transition(transactionId, "PLANNED", "PREFLIGHTING", "checking both nodes");
                state = "PREFLIGHTING";
                plan = this.store.transaction(transactionId).orElseThrow();
            }
            if (!"PREFLIGHTING".equals(state)) {
                return;
            }
            final PlanContext context = this.context(plan);
            if (!this.control.nodeConnected(context.source().nodeId())
                || !this.control.nodeConnected(context.target().nodeId())) {
                throw new IOException("both authenticated nodes must be connected");
            }
            final WorldTransactionCodec.SignedManifest signed =
                WorldTransactionCodec.sign(context.manifest(), this.signingKey);
            final Request status = new Request(Operation.STATUS, signed);
            final CompletableFuture<Response> sourceStatus =
                this.control.transaction(context.source().nodeId(), status, NODE_TIMEOUT);
            final CompletableFuture<Response> targetStatus =
                this.control.transaction(context.target().nodeId(), status, NODE_TIMEOUT);
            final Response source = await(sourceStatus);
            final Response target = await(targetStatus);
            requireReadyStatus(source, context.manifest(), "source");
            requireReadyStatus(target, context.manifest(), "target");
            final long required = safetyMargin(context.manifest().estimatedBytes());
            if (source.usableBytes() < required || target.usableBytes() < required) {
                throw new IOException("both nodes require at least " + required
                    + " usable bytes for staging with the 20% safety margin");
            }
            this.store.transition(
                transactionId,
                "PREFLIGHTING",
                "PREFLIGHT_READY",
                "both signed node preflight checks passed"
            );
            this.logger.info("Shardingbase transaction {} passed two-node preflight", transactionId);
        } catch (final Exception exception) {
            final String detail = safeMessage(exception);
            this.logger.warn("Shardingbase transaction {} failed preflight: {}", transactionId, detail);
            if ("PREFLIGHTING".equals(state)) {
                try {
                    this.store.transition(transactionId, "PREFLIGHTING", "FAILED", detail);
                } catch (final IOException persistenceFailure) {
                    this.logger.error("Unable to persist failed Shardingbase transaction {}", transactionId,
                        persistenceFailure);
                }
            }
        }
    }

    private PlanContext context(final WorldPlannerStore.TransactionPlan plan) throws IOException {
        final List<BackendRegistry.BackendTarget> backends = this.registry.backends();
        if (backends.size() != 2) {
            throw new IOException("exactly two registered backends are required");
        }
        final BackendRegistry.BackendTarget source = backends.stream()
            .filter(candidate -> candidate.serverId().equals(plan.session().backendId()))
            .findFirst().orElseThrow(() -> new IOException("planner source backend is not registered"));
        final BackendRegistry.BackendTarget target = backends.stream()
            .filter(candidate -> !candidate.serverId().equals(source.serverId()))
            .findFirst().orElseThrow(() -> new IOException("planner target backend is not registered"));
        final String negativeNode = nodeForBackend(backends, plan.negativeBackendId());
        final String positiveNode = nodeForBackend(backends, plan.positiveBackendId());
        final WorldPlannerStore.Session session = plan.session();
        return new PlanContext(source, target, new Manifest(
            plan.transactionId(),
            source.nodeId(),
            target.nodeId(),
            source.serverId(),
            target.serverId(),
            session.worldKey(),
            session.worldDirectory(),
            session.worldId(),
            session.worldSeed(),
            session.dataVersion(),
            WorldTransactionCodec.Axis.valueOf(plan.axis()),
            plan.cutChunk(),
            negativeNode,
            positiveNode,
            session.estimatedBytes()
        ));
    }

    private static String nodeForBackend(
        final List<BackendRegistry.BackendTarget> backends,
        final String backendId
    ) throws IOException {
        return backends.stream()
            .filter(candidate -> candidate.serverId().equals(backendId))
            .map(BackendRegistry.BackendTarget::nodeId)
            .findFirst()
            .orElseThrow(() -> new IOException("shard assignment references an unregistered backend"));
    }

    private static Response await(final CompletableFuture<Response> future) throws IOException {
        try {
            return future.get(NODE_TIMEOUT.plusSeconds(2).toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during node preflight", exception);
        } catch (final ExecutionException exception) {
            throw new IOException("Node preflight request failed", exception.getCause());
        } catch (final TimeoutException exception) {
            throw new IOException("Node preflight request did not complete", exception);
        }
    }

    private static void requireReadyStatus(
        final Response response,
        final Manifest manifest,
        final String role
    ) throws IOException {
        if (response.outcome() != Outcome.SUCCESS || !response.backendRunning()) {
            throw new IOException(role + " node/backend is not ready: " + response.detail());
        }
        if (!java.security.MessageDigest.isEqual(
            response.manifestDigest(),
            WorldTransactionCodec.digest(manifest)
        )) {
            throw new IOException(role + " node acknowledged a different manifest");
        }
    }

    static long safetyMargin(final long bytes) {
        if (bytes > Long.MAX_VALUE / 6L * 5L) {
            return Long.MAX_VALUE;
        }
        return bytes + (bytes + 4L) / 5L;
    }

    private static String safeMessage(final Exception exception) {
        final String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.worker.shutdownNow();
        }
    }

    private record PlanContext(
        BackendRegistry.BackendTarget source,
        BackendRegistry.BackendTarget target,
        Manifest manifest
    ) {
    }
}
