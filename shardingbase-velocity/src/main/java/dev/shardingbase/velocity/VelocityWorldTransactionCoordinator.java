package dev.shardingbase.velocity;

import com.velocitypowered.api.proxy.ProxyServer;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;

/** Durable two-node authority for preflight, offline commit, health validation, and rollback. */
final class VelocityWorldTransactionCoordinator implements AutoCloseable {
    private static final Duration SHORT_NODE_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration LONG_NODE_TIMEOUT = Duration.ofMinutes(30);
    private static final Duration HEALTH_TIMEOUT = Duration.ofSeconds(90);
    private static final Set<String> RECOVERABLE_STATES = Set.of(
        "PLANNED", "PREFLIGHTING", "PREFLIGHT_READY", "MAINTENANCE", "AUTHORIZED", "STOPPED",
        "BACKUPS_READY", "RELAYED", "TARGET_INSTALLED", "SOURCE_COMMITTED", "STARTING_TARGET",
        "TARGET_HEALTHY", "STARTING_SOURCE", "BOTH_HEALTHY", "FINALIZING", "ROLLING_BACK"
    );
    private static final Set<String> MUTATING_STATES = Set.of(
        "AUTHORIZED", "STOPPED", "BACKUPS_READY", "RELAYED", "TARGET_INSTALLED",
        "SOURCE_COMMITTED", "STARTING_TARGET", "TARGET_HEALTHY", "STARTING_SOURCE",
        "BOTH_HEALTHY"
    );

    private final ProxyServer proxy;
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
        final ProxyServer proxy,
        final VelocityConfiguration configuration,
        final WorldPlannerStore store,
        final BackendRegistry registry,
        final ControlServer control,
        final Logger logger
    ) throws IOException {
        this.proxy = proxy;
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
        final String[] recoverable = RECOVERABLE_STATES.toArray(String[]::new);
        for (final WorldPlannerStore.TransactionPlan plan : store.transactionsIn(recoverable)) {
            this.submit(plan.transactionId());
        }
    }

    void submit(final UUID transactionId) {
        if (this.closed.get()) {
            return;
        }
        try {
            this.worker.execute(() -> this.run(transactionId));
        } catch (final RuntimeException exception) {
            if (!this.closed.get()) {
                this.logger.warn("Unable to schedule Shardingbase transaction {}", transactionId, exception);
            }
        }
    }

    private void run(final UUID transactionId) {
        try {
            while (!this.closed.get()) {
                final WorldPlannerStore.TransactionPlan plan = this.store.transaction(transactionId)
                    .orElseThrow(() -> new IOException("World transaction plan does not exist"));
                final PlanContext context = this.context(plan);
                final WorldTransactionCodec.SignedManifest signed =
                    WorldTransactionCodec.sign(context.manifest(), this.signingKey);
                switch (plan.state()) {
                    case "PLANNED", "PREFLIGHTING" -> this.preflight(plan, context, signed);
                    case "PREFLIGHT_READY" -> this.enterMaintenance(plan, context);
                    case "MAINTENANCE" -> this.authorize(plan, context, signed);
                    case "AUTHORIZED" -> this.stopBackends(plan, context, signed);
                    case "STOPPED" -> this.prepareBackups(plan, context, signed);
                    case "BACKUPS_READY" -> this.relay(plan, context, signed);
                    case "RELAYED" -> this.installTarget(plan, context, signed);
                    case "TARGET_INSTALLED" -> this.commitSource(plan, context, signed);
                    case "SOURCE_COMMITTED" -> this.startTarget(plan, context, signed);
                    case "STARTING_TARGET" -> this.awaitTarget(plan, context, signed);
                    case "TARGET_HEALTHY" -> this.startSource(plan, context, signed);
                    case "STARTING_SOURCE" -> this.awaitSource(plan, context, signed);
                    case "BOTH_HEALTHY" -> this.beginFinalizing(plan);
                    case "FINALIZING" -> this.finish(plan, context, signed);
                    case "ROLLING_BACK" -> {
                        this.rollback(plan, context, signed);
                        return;
                    }
                    default -> {
                        return;
                    }
                }
            }
        } catch (final Exception exception) {
            this.fail(transactionId, exception);
        }
    }

    private void preflight(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        if ("PLANNED".equals(plan.state())) {
            this.store.transition(plan.transactionId(), "PLANNED", "PREFLIGHTING", "checking both nodes");
        }
        requireConnected(context);
        final Request statusRequest = new Request(Operation.STATUS, signed);
        final Response source = await(this.control.transaction(
            context.source().nodeId(), statusRequest, SHORT_NODE_TIMEOUT
        ), SHORT_NODE_TIMEOUT);
        final Response target = await(this.control.transaction(
            context.target().nodeId(), statusRequest, SHORT_NODE_TIMEOUT
        ), SHORT_NODE_TIMEOUT);
        requireOutcome(source, Outcome.SUCCESS, context.manifest(), "source preflight");
        requireOutcome(target, Outcome.SUCCESS, context.manifest(), "target preflight");
        if (!source.backendRunning() || !target.backendRunning()) {
            throw new IOException("both backend child processes must be running");
        }
        final long required = safetyMargin(context.manifest().estimatedBytes());
        if (source.usableBytes() < required || target.usableBytes() < required) {
            throw new IOException("both nodes require at least " + required
                + " usable bytes for staging with the 20% safety margin");
        }
        this.store.transition(
            plan.transactionId(), "PREFLIGHTING", "PREFLIGHT_READY",
            "both signed node preflight checks passed"
        );
    }

    private void enterMaintenance(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context
    ) throws IOException {
        final String reason = "world transaction " + plan.transactionId() + " is preparing an offline shard cut";
        this.registry.setPairStatus(context.backendIds(), "MAINTENANCE", reason);
        for (final com.velocitypowered.api.proxy.Player player : this.proxy.getAllPlayers()) {
            final String current = player.getCurrentServer()
                .map(connection -> connection.getServerInfo().getName()).orElse("");
            if (current.equals(context.source().serverName()) || current.equals(context.target().serverName())) {
                player.disconnect(Component.text("Shardingbase maintenance: " + reason));
            }
        }
        this.store.transition(plan.transactionId(), "PREFLIGHT_READY", "MAINTENANCE", reason);
    }

    private void authorize(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        requireConnected(context);
        final Request request = new Request(Operation.AUTHORIZE_AND_SAVE, signed);
        final ResponsePair responses = requestBoth(context, request, LONG_NODE_TIMEOUT);
        requireOutcome(responses.source(), Outcome.READY, context.manifest(), "source save authorization");
        requireOutcome(responses.target(), Outcome.READY, context.manifest(), "target save authorization");
        this.store.transition(
            plan.transactionId(), "MAINTENANCE", "AUTHORIZED",
            "both backends saved, flushed, and authorized the signed manifest"
        );
    }

    private void stopBackends(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final ResponsePair responses = requestBoth(
            context, new Request(Operation.STOP_BACKEND, signed), Duration.ofSeconds(75)
        );
        requireOutcome(responses.source(), Outcome.SUCCESS, context.manifest(), "source stop");
        requireOutcome(responses.target(), Outcome.SUCCESS, context.manifest(), "target stop");
        if (responses.source().backendRunning() || responses.target().backendRunning()) {
            throw new IOException("both backends must report stopped before offline work");
        }
        this.store.transition(plan.transactionId(), "AUTHORIZED", "STOPPED", "both backends stopped cleanly");
    }

    private void prepareBackups(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final CompletableFuture<Response> source = this.control.transaction(
            context.source().nodeId(), new Request(Operation.PREPARE_SOURCE, signed), LONG_NODE_TIMEOUT
        );
        final CompletableFuture<Response> target = this.control.transaction(
            context.target().nodeId(), new Request(Operation.PREPARE_TARGET, signed), LONG_NODE_TIMEOUT
        );
        final Response sourceResponse = await(source, LONG_NODE_TIMEOUT);
        final Response targetResponse = await(target, LONG_NODE_TIMEOUT);
        requireOutcome(sourceResponse, Outcome.SUCCESS, context.manifest(), "source backup/split");
        requireOutcome(targetResponse, Outcome.SUCCESS, context.manifest(), "target rollback point");
        this.store.transition(
            plan.transactionId(), "STOPPED", "BACKUPS_READY",
            "both rollback points are complete and the source split is prepared"
        );
    }

    private void relay(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final Response response = await(this.control.transaction(
            context.source().nodeId(), new Request(Operation.RELAY_TARGET, signed), LONG_NODE_TIMEOUT
        ), LONG_NODE_TIMEOUT);
        requireOutcome(response, Outcome.SUCCESS, context.manifest(), "target shard relay");
        this.store.transition(plan.transactionId(), "BACKUPS_READY", "RELAYED", response.detail());
    }

    private void installTarget(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final Response response = await(this.control.transaction(
            context.target().nodeId(), new Request(Operation.INSTALL_TARGET, signed), LONG_NODE_TIMEOUT
        ), LONG_NODE_TIMEOUT);
        requireOutcome(response, Outcome.SUCCESS, context.manifest(), "target install");
        this.store.transition(plan.transactionId(), "RELAYED", "TARGET_INSTALLED", response.detail());
    }

    private void commitSource(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final Response response = await(this.control.transaction(
            context.source().nodeId(), new Request(Operation.COMMIT_SOURCE, signed), LONG_NODE_TIMEOUT
        ), LONG_NODE_TIMEOUT);
        requireOutcome(response, Outcome.SUCCESS, context.manifest(), "source commit");
        this.store.transition(plan.transactionId(), "TARGET_INSTALLED", "SOURCE_COMMITTED", response.detail());
    }

    private void startTarget(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        this.registry.clearPairHealth(context.backendIds());
        final Response response = await(this.control.transaction(
            context.target().nodeId(), new Request(Operation.RESTART_BACKEND, signed), SHORT_NODE_TIMEOUT
        ), SHORT_NODE_TIMEOUT);
        requireOutcome(response, Outcome.SUCCESS, context.manifest(), "target restart");
        this.store.transition(plan.transactionId(), "SOURCE_COMMITTED", "STARTING_TARGET", "starting target first");
    }

    private void awaitTarget(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        awaitHealth(context.target(), context.manifest(), signed);
        this.store.transition(
            plan.transactionId(), "STARTING_TARGET", "TARGET_HEALTHY",
            "target backend restarted and revalidated"
        );
    }

    private void startSource(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final Response response = await(this.control.transaction(
            context.source().nodeId(), new Request(Operation.RESTART_BACKEND, signed), SHORT_NODE_TIMEOUT
        ), SHORT_NODE_TIMEOUT);
        requireOutcome(response, Outcome.SUCCESS, context.manifest(), "source restart");
        this.store.transition(plan.transactionId(), "TARGET_HEALTHY", "STARTING_SOURCE", "starting source second");
    }

    private void awaitSource(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        awaitHealth(context.source(), context.manifest(), signed);
        this.store.transition(
            plan.transactionId(), "STARTING_SOURCE", "BOTH_HEALTHY",
            "both sharded backends restarted and revalidated"
        );
    }

    private void beginFinalizing(final WorldPlannerStore.TransactionPlan plan) throws IOException {
        this.store.transition(
            plan.transactionId(), "BOTH_HEALTHY", "FINALIZING",
            "persisting completion on both nodes"
        );
    }

    private void finish(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final ResponsePair responses = requestBoth(
            context, new Request(Operation.COMPLETE, signed), SHORT_NODE_TIMEOUT
        );
        requireOutcome(responses.source(), Outcome.SUCCESS, context.manifest(), "source completion");
        requireOutcome(responses.target(), Outcome.SUCCESS, context.manifest(), "target completion");
        this.registry.setPairStatus(context.backendIds(), "ONLINE", "world transaction complete");
        this.store.transition(
            plan.transactionId(), "FINALIZING", "COMPLETE",
            "both shard manifests are healthy and maintenance was released"
        );
        this.logger.info("Shardingbase transaction {} completed", plan.transactionId());
    }

    private void awaitHealth(
        final BackendRegistry.BackendTarget backend,
        final Manifest manifest,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final long deadline = System.nanoTime() + HEALTH_TIMEOUT.toNanos();
        IOException lastFailure = null;
        while (System.nanoTime() < deadline && !this.closed.get()) {
            try {
                final Response response = await(this.control.transaction(
                    backend.nodeId(), new Request(Operation.STATUS, signed), SHORT_NODE_TIMEOUT
                ), SHORT_NODE_TIMEOUT);
                requireOutcome(response, Outcome.SUCCESS, manifest, backend.serverName() + " health");
                if (response.backendRunning() && this.registry.lastSeen(backend.serverId()) > 0L) {
                    return;
                }
                if (!response.backendRunning() && response.lastExitCode() >= 0) {
                    throw new IOException("backend exited during health check with code " + response.lastExitCode());
                }
            } catch (final IOException exception) {
                lastFailure = exception;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for backend health", exception);
            }
        }
        throw new IOException("Backend health check timed out"
            + (lastFailure == null ? "" : ": " + lastFailure.getMessage()), lastFailure);
    }

    private void fail(final UUID transactionId, final Exception failure) {
        final String detail = safeMessage(failure);
        this.logger.error("Shardingbase transaction {} failed: {}", transactionId, detail, failure);
        try {
            final WorldPlannerStore.TransactionPlan plan = this.store.transaction(transactionId).orElse(null);
            if (plan == null || "COMPLETE".equals(plan.state()) || "ROLLED_BACK".equals(plan.state())
                || "FAILED".equals(plan.state())) {
                return;
            }
            if ("FINALIZING".equals(plan.state())) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (final InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return;
                }
                this.submit(transactionId);
                return;
            }
            if (MUTATING_STATES.contains(plan.state()) || "ROLLING_BACK".equals(plan.state())) {
                if (!"ROLLING_BACK".equals(plan.state())) {
                    this.store.transition(transactionId, plan.state(), "ROLLING_BACK", detail);
                }
                final PlanContext context = this.context(this.store.transaction(transactionId).orElseThrow());
                final WorldTransactionCodec.SignedManifest signed =
                    WorldTransactionCodec.sign(context.manifest(), this.signingKey);
                this.rollback(this.store.transaction(transactionId).orElseThrow(), context, signed);
                return;
            }
            if ("MAINTENANCE".equals(plan.state())) {
                final PlanContext context = this.context(plan);
                final WorldTransactionCodec.SignedManifest signed =
                    WorldTransactionCodec.sign(context.manifest(), this.signingKey);
                final ResponsePair responses = requestBoth(
                    context, new Request(Operation.ROLLBACK, signed), SHORT_NODE_TIMEOUT
                );
                requireOutcome(responses.source(), Outcome.SUCCESS, context.manifest(), "source authorization abort");
                requireOutcome(responses.target(), Outcome.SUCCESS, context.manifest(), "target authorization abort");
                final ResponsePair completions = requestBoth(
                    context, new Request(Operation.COMPLETE, signed), SHORT_NODE_TIMEOUT
                );
                requireOutcome(completions.source(), Outcome.SUCCESS, context.manifest(), "source abort completion");
                requireOutcome(completions.target(), Outcome.SUCCESS, context.manifest(), "target abort completion");
                this.registry.setPairStatus(context.backendIds(), "ONLINE", "transaction aborted before backend stop");
                this.store.transition(transactionId, "MAINTENANCE", "ROLLED_BACK", detail);
                return;
            }
            this.store.transition(transactionId, plan.state(), "FAILED", detail);
        } catch (final Exception recoveryFailure) {
            this.logger.error(
                "Shardingbase transaction {} rollback failed; maintenance remains locked",
                transactionId,
                recoveryFailure
            );
        }
    }

    private void rollback(
        final WorldPlannerStore.TransactionPlan plan,
        final PlanContext context,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        final Request rollback = new Request(Operation.ROLLBACK, signed);
        final ResponsePair responses = requestBoth(context, rollback, LONG_NODE_TIMEOUT);
        requireOutcome(responses.source(), Outcome.SUCCESS, context.manifest(), "source rollback");
        requireOutcome(responses.target(), Outcome.SUCCESS, context.manifest(), "target rollback");
        this.restartAfterRollback(
            context.source(), responses.source().backendRunning(), context.manifest(), signed
        );
        this.restartAfterRollback(
            context.target(), responses.target().backendRunning(), context.manifest(), signed
        );
        awaitHealth(context.source(), context.manifest(), signed);
        awaitHealth(context.target(), context.manifest(), signed);
        final ResponsePair completions = requestBoth(
            context, new Request(Operation.COMPLETE, signed), SHORT_NODE_TIMEOUT
        );
        requireOutcome(completions.source(), Outcome.SUCCESS, context.manifest(), "source rollback completion");
        requireOutcome(completions.target(), Outcome.SUCCESS, context.manifest(), "target rollback completion");
        this.registry.setPairStatus(context.backendIds(), "ONLINE", "world transaction rolled back");
        this.store.transition(
            plan.transactionId(), "ROLLING_BACK", "ROLLED_BACK",
            "both rollback points restored; failed transaction retained"
        );
    }

    private void restartAfterRollback(
        final BackendRegistry.BackendTarget backend,
        final boolean running,
        final Manifest manifest,
        final WorldTransactionCodec.SignedManifest signed
    ) throws IOException {
        if (running) {
            return;
        }
        this.registry.clearHealth(backend.serverId());
        final Response response = await(this.control.transaction(
            backend.nodeId(), new Request(Operation.RESTART_BACKEND, signed), SHORT_NODE_TIMEOUT
        ), SHORT_NODE_TIMEOUT);
        requireOutcome(response, Outcome.SUCCESS, manifest, backend.serverName() + " rollback restart");
    }

    private ResponsePair requestBoth(
        final PlanContext context,
        final Request request,
        final Duration timeout
    ) throws IOException {
        requireConnected(context);
        final CompletableFuture<Response> source =
            this.control.transaction(context.source().nodeId(), request, timeout);
        final CompletableFuture<Response> target =
            this.control.transaction(context.target().nodeId(), request, timeout);
        return new ResponsePair(await(source, timeout), await(target, timeout));
    }

    private static void requireConnected(final PlanContext context) throws IOException {
        // Connection state is checked by ControlServer.transaction; this method documents the paired invariant.
        if (context.source().nodeId().equals(context.target().nodeId())) {
            throw new IOException("source and target must use distinct authenticated nodes");
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

    private static Response await(final CompletableFuture<Response> future, final Duration timeout)
        throws IOException {
        try {
            return future.get(timeout.plusSeconds(2).toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during node transaction request", exception);
        } catch (final ExecutionException exception) {
            throw new IOException("Node transaction request failed", exception.getCause());
        } catch (final TimeoutException exception) {
            throw new IOException("Node transaction request did not complete", exception);
        }
    }

    private static void requireOutcome(
        final Response response,
        final Outcome required,
        final Manifest manifest,
        final String phase
    ) throws IOException {
        if (response.outcome() != required) {
            throw new IOException(phase + " failed: " + response.detail());
        }
        if (!java.security.MessageDigest.isEqual(
            response.manifestDigest(),
            WorldTransactionCodec.digest(manifest)
        )) {
            throw new IOException(phase + " acknowledged a different manifest");
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
        private List<String> backendIds() {
            return List.of(this.source.serverId(), this.target.serverId());
        }
    }

    private record ResponsePair(Response source, Response target) {
    }
}
