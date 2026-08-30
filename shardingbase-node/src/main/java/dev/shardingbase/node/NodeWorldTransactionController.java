package dev.shardingbase.node;

import dev.shardingbase.node.world.OfflineWorldTransactionPreparer;
import dev.shardingbase.node.world.OfflineTargetTransactionPreparer;
import dev.shardingbase.node.world.ShardAxis;
import dev.shardingbase.node.world.ShardManifestWriter;
import dev.shardingbase.node.world.ShardSide;
import dev.shardingbase.node.world.TransferTreeManifest;
import dev.shardingbase.node.world.WorldInstallationEngine;
import dev.shardingbase.node.world.WorldTransactionJournal;
import dev.shardingbase.node.world.TransactionPhase;
import dev.shardingbase.protocol.MessageType;
import dev.shardingbase.protocol.ProtocolChannel;
import dev.shardingbase.protocol.ProtocolFrame;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import dev.shardingbase.protocol.WorldTransactionCodec;
import dev.shardingbase.protocol.WorldTransactionCodec.Manifest;
import dev.shardingbase.protocol.WorldTransactionCodec.Operation;
import dev.shardingbase.protocol.WorldTransactionCodec.Outcome;
import dev.shardingbase.protocol.WorldTransactionCodec.Request;
import dev.shardingbase.protocol.WorldTransactionCodec.Response;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.HexFormat;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Executes signed, backend-authorized world transaction phases on one node. */
final class NodeWorldTransactionController implements AutoCloseable {
    static final String SIGNING_KEY_ENVIRONMENT = "SHARDINGBASE_TRANSACTION_KEY";
    static final String WORLD_ROOT_ENVIRONMENT = "SHARDINGBASE_WORLD_ROOT";
    static final String BACKUP_ROOT_ENVIRONMENT = "SHARDINGBASE_BACKUP_ROOT";
    static final String TRANSACTION_ROOT_ENVIRONMENT = "SHARDINGBASE_TRANSACTION_ROOT";
    private static final Duration BACKEND_AUTHORIZATION_TIMEOUT = Duration.ofSeconds(30);

    private final ProxyValidationClient proxy;
    private final LocalBackendController local;
    private final BackendProcess backend;
    private final ResumableFileSender fileSender;
    private final String nodeId;
    private final byte[] signingKey;
    private final String unavailableDetail;
    private final Path worldRoot;
    private final Path backupRoot;
    private final Path transactionRoot;
    private final Path stagingRoot;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(task -> Thread.ofPlatform()
        .daemon(true)
        .name("Shardingbase World Transaction")
        .unstarted(task));
    private final ConcurrentHashMap<UUID, CompletableFuture<Response>> backendAuthorizations =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, byte[]> authorized = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, OfflineWorldTransactionPreparer.PreparedTransaction> prepared =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, OfflineTargetTransactionPreparer.PreparedTarget> preparedTargets =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, InstalledRecord> installed = new ConcurrentHashMap<>();
    private final AtomicReference<UUID> activeTransaction = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private NodeWorldTransactionController(
        final ProxyValidationClient proxy,
        final LocalBackendController local,
        final BackendProcess backend,
        final Map<String, String> environment
    ) throws IOException {
        this.proxy = proxy;
        this.local = local;
        this.backend = backend;
        this.fileSender = new ResumableFileSender(proxy);
        final String configuredNodeId = environment.get(ProxyValidationClient.NODE_ID_ENVIRONMENT_VARIABLE);
        final String configuredSigningKey = environment.get(SIGNING_KEY_ENVIRONMENT);
        this.nodeId = configuredNodeId == null ? "" : configuredNodeId;
        this.signingKey = configuredSigningKey == null || configuredSigningKey.isBlank()
            ? null
            : signingKey(configuredSigningKey);
        this.unavailableDetail = this.nodeId.isBlank()
            ? ProxyValidationClient.NODE_ID_ENVIRONMENT_VARIABLE + " is required for world transactions"
            : this.signingKey == null
                ? SIGNING_KEY_ENVIRONMENT + " is required for world transactions"
                : null;
        final Path workingDirectory = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        this.worldRoot = configuredPath(environment.get(WORLD_ROOT_ENVIRONMENT), workingDirectory, workingDirectory);
        this.backupRoot = configuredPath(
            environment.get(BACKUP_ROOT_ENVIRONMENT),
            workingDirectory,
            workingDirectory.resolve("shardingbase-backups")
        );
        this.transactionRoot = configuredPath(
            environment.get(TRANSACTION_ROOT_ENVIRONMENT),
            workingDirectory,
            workingDirectory.resolve("shardingbase-transactions")
        );
        this.stagingRoot = configuredPath(
            environment.get(NodeFileTransferHandler.STAGING_ROOT_ENVIRONMENT),
            workingDirectory,
            workingDirectory.resolve("shardingbase-staging")
        );
        if (this.backupRoot.equals(this.transactionRoot)
            || this.backupRoot.startsWith(this.transactionRoot)
            || this.transactionRoot.startsWith(this.backupRoot)) {
            throw new IOException("Backup and transaction roots must be separate, non-nested paths");
        }
    }

    static NodeWorldTransactionController start(
        final ProxyValidationClient proxy,
        final LocalBackendController local,
        final BackendProcess backend
    ) throws IOException {
        final NodeWorldTransactionController controller =
            new NodeWorldTransactionController(proxy, local, backend, System.getenv());
        local.localRequestHandler(controller::handleLocalRequest);
        proxy.pushHandler(ProtocolChannel.WORLD_TRANSACTION, controller::submit);
        return controller;
    }

    private void submit(final ProtocolFrame frame) {
        try {
            this.worker.execute(() -> this.process(frame));
        } catch (final RuntimeException exception) {
            this.respondFailure(frame, null, Operation.STATUS, "transaction worker is unavailable");
        }
    }

    private void process(final ProtocolFrame frame) {
        Request request = null;
        try {
            if (frame.messageType() != MessageType.WORLD_TRANSACTION_REQUEST) {
                throw new IOException("unexpected world transaction message type");
            }
            request = WorldTransactionCodec.decodeRequest(frame.payload());
            final Manifest manifest = request.signedManifest().manifest();
            if (this.unavailableDetail != null) {
                throw new IOException(this.unavailableDetail);
            }
            if (!this.nodeId.equals(manifest.sourceNodeId()) && !this.nodeId.equals(manifest.targetNodeId())) {
                throw new IOException("transaction manifest does not include this node");
            }
            if (!WorldTransactionCodec.verify(request.signedManifest(), this.signingKey)) {
                throw new IOException("world transaction manifest signature is invalid");
            }
            if (request.operation() != Operation.STATUS) {
                this.claim(manifest.transactionId());
            }
            final Response response = switch (request.operation()) {
                case STATUS -> this.status(request, Outcome.SUCCESS, "node transaction status");
                case AUTHORIZE_AND_SAVE -> this.authorize(frame, request);
                case STOP_BACKEND -> this.stop(request);
                case PREPARE_SOURCE -> this.prepareSource(request);
                case RESTART_BACKEND -> this.restart(request);
                case PREPARE_TARGET -> this.prepareTarget(request);
                case RELAY_TARGET -> this.relayTarget(request);
                case INSTALL_TARGET -> this.installTarget(request);
                case COMMIT_SOURCE -> this.commitSource(request);
                case ROLLBACK -> this.rollback(request);
                case COMPLETE -> this.complete(request);
            };
            this.proxy.respond(frame, MessageType.WORLD_TRANSACTION_RESPONSE,
                WorldTransactionCodec.encodeResponse(response));
        } catch (final Exception exception) {
            final Operation operation = request == null ? Operation.STATUS : request.operation();
            final UUID transactionId = request == null ? null : request.signedManifest().manifest().transactionId();
            this.respondFailure(frame, transactionId, operation, safeMessage(exception));
        }
    }

    private Response authorize(final ProtocolFrame frame, final Request request) throws IOException {
        if (!this.backend.status().running()) {
            return this.status(request, Outcome.REJECTED, "local backend is not running");
        }
        final UUID transactionId = request.signedManifest().manifest().transactionId();
        final byte[] digest = WorldTransactionCodec.digest(request.signedManifest().manifest());
        final byte[] existing = this.authorized.get(transactionId);
        if (existing != null) {
            return this.status(
                request,
                Arrays.equals(existing, digest) ? Outcome.READY : Outcome.REJECTED,
                Arrays.equals(existing, digest)
                    ? "backend already authorized this exact manifest"
                    : "transaction ID was already authorized with a different manifest"
            );
        }
        final CompletableFuture<Response> backendReady = new CompletableFuture<>();
        if (this.backendAuthorizations.putIfAbsent(transactionId, backendReady) != null) {
            return this.status(request, Outcome.REJECTED, "backend authorization is already pending");
        }
        try {
            this.local.enqueueBackendPush(frame);
            final Response response;
            try {
                response = backendReady.get(BACKEND_AUTHORIZATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for backend transaction authorization", exception);
            } catch (final TimeoutException exception) {
                throw new IOException("Local backend did not authorize the transaction within 30 seconds", exception);
            } catch (final ExecutionException exception) {
                throw new IOException("Local backend transaction authorization failed", exception.getCause());
            }
            if (!transactionId.equals(response.transactionId())
                || response.operation() != Operation.AUTHORIZE_AND_SAVE
                || response.outcome() != Outcome.READY
                || !Arrays.equals(digest, response.manifestDigest())) {
                return this.status(request, Outcome.REJECTED,
                    "local backend did not acknowledge the exact signed manifest");
            }
            this.persistAuthorization(transactionId, digest);
            this.authorized.put(transactionId, digest.clone());
            return this.status(request, Outcome.READY, "local backend saved, flushed, and authorized the manifest");
        } finally {
            this.backendAuthorizations.remove(transactionId, backendReady);
        }
    }

    private Response stop(final Request request) throws IOException {
        this.requireAuthorization(request);
        try {
            if (!this.backend.stopGracefully()) {
                return this.status(request, Outcome.REJECTED,
                    "backend did not stop within 60 seconds; no force-kill was attempted");
            }
            return this.status(request, Outcome.SUCCESS, "backend stopped cleanly");
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for the backend to stop", exception);
        }
    }

    private Response prepareSource(final Request request) throws IOException {
        this.requireAuthorization(request);
        final Manifest manifest = request.signedManifest().manifest();
        if (!this.nodeId.equals(manifest.sourceNodeId())) {
            return this.status(request, Outcome.REJECTED, "only the declared source node may split the world");
        }
        if (this.backend.status().running()) {
            return this.status(request, Outcome.REJECTED, "backend must be stopped before offline preparation");
        }
        if (this.sourcePrepared(manifest) != null) {
            return this.status(request, Outcome.SUCCESS, "source world is already prepared");
        }
        final Path world = this.resolveWorld(manifest.worldDirectory());
        if (!Files.isDirectory(world)) {
            return this.status(request, Outcome.REJECTED, "configured source world directory does not exist");
        }
        Files.createDirectories(this.backupRoot);
        Files.createDirectories(this.transactionRoot);
        final long required = safetyMargin(saturatedAdd(directoryBytes(world), manifest.estimatedBytes()));
        final long usable = usableBytes(this.backupRoot);
        if (usable < required) {
            return this.status(request, Outcome.REJECTED,
                "insufficient staging/backup space: requires " + required + " bytes with safety margin");
        }
        final OfflineWorldTransactionPreparer.PreparedTransaction result =
            OfflineWorldTransactionPreparer.prepare(
                new OfflineWorldTransactionPreparer.Plan(
                    manifest.transactionId(),
                    world,
                    this.backupRoot,
                    this.transactionRoot,
                    manifest.axis() == WorldTransactionCodec.Axis.X ? ShardAxis.X : ShardAxis.Z,
                    manifest.cutChunk()
                ),
                manifest.transactionId(),
                manifest.transactionId(),
                true
            );
        this.prepared.put(manifest.transactionId(), result);
        return this.status(
            request,
            Outcome.SUCCESS,
            "source prepared " + result.summary().regionFiles() + " region files ("
                + result.summary().negativeChunkEntries() + " negative, "
                + result.summary().positiveChunkEntries() + " positive chunk entries)"
        );
    }

    private Response restart(final Request request) throws IOException {
        this.requireAuthorization(request);
        if (this.backend.status().running()) {
            return this.status(request, Outcome.SUCCESS, "backend is already running");
        }
        this.backend.restart();
        this.advanceRestartJournal(request.signedManifest().manifest());
        return this.status(request, Outcome.SUCCESS, "backend restart launched");
    }

    private void advanceRestartJournal(final Manifest manifest) throws IOException {
        final Path journalPath;
        if (this.nodeId.equals(manifest.sourceNodeId())) {
            final OfflineWorldTransactionPreparer.PreparedTransaction source = this.sourcePrepared(manifest);
            journalPath = source == null ? null : source.journal();
        } else {
            final OfflineTargetTransactionPreparer.PreparedTarget target = this.targetPrepared(manifest);
            journalPath = target == null ? null : target.journal();
        }
        if (journalPath == null) {
            return;
        }
        final WorldTransactionJournal journal = WorldTransactionJournal.load(journalPath);
        if (this.nodeId.equals(manifest.targetNodeId()) && journal.phase() == TransactionPhase.TARGET_PREPARED) {
            journal.advance(TransactionPhase.SOURCE_COMMITTED);
        }
        if (journal.phase() == TransactionPhase.SOURCE_COMMITTED) {
            journal.advance(TransactionPhase.STARTING_TARGET);
        }
        if (this.nodeId.equals(manifest.sourceNodeId()) && journal.phase() == TransactionPhase.STARTING_TARGET) {
            journal.advance(TransactionPhase.STARTING_SOURCE);
        }
    }

    private Response prepareTarget(final Request request) throws IOException {
        this.requireAuthorization(request);
        final Manifest manifest = request.signedManifest().manifest();
        if (!this.nodeId.equals(manifest.targetNodeId())) {
            return this.status(request, Outcome.REJECTED, "only the declared target node may prepare the target");
        }
        if (this.backend.status().running()) {
            return this.status(request, Outcome.REJECTED, "backend must be stopped before target preparation");
        }
        if (this.targetPrepared(manifest) != null) {
            return this.status(request, Outcome.SUCCESS, "target rollback point is already prepared");
        }
        final Path world = this.resolveWorld(manifest.worldDirectory());
        Files.createDirectories(this.backupRoot);
        Files.createDirectories(this.transactionRoot);
        final long existingBytes = Files.isDirectory(world) ? directoryBytes(world) : 0L;
        final long required = safetyMargin(saturatedAdd(existingBytes, manifest.estimatedBytes()));
        if (usableBytes(this.backupRoot) < required) {
            return this.status(request, Outcome.REJECTED,
                "insufficient target backup/staging space: requires " + required + " bytes with safety margin");
        }
        final OfflineTargetTransactionPreparer.PreparedTarget result =
            OfflineTargetTransactionPreparer.prepare(
                new OfflineTargetTransactionPreparer.Plan(
                    manifest.transactionId(),
                    world,
                    this.backupRoot,
                    this.transactionRoot
                ),
                manifest.transactionId(),
                manifest.transactionId(),
                true
            );
        this.preparedTargets.put(manifest.transactionId(), result);
        return this.status(
            request,
            Outcome.SUCCESS,
            result.worldInitiallyAbsent()
                ? "target absence rollback point prepared"
                : "complete target world backup prepared"
        );
    }

    private Response relayTarget(final Request request) throws IOException {
        this.requireAuthorization(request);
        final Manifest manifest = request.signedManifest().manifest();
        if (!this.nodeId.equals(manifest.sourceNodeId())) {
            return this.status(request, Outcome.REJECTED, "only the source node may relay a shard tree");
        }
        if (this.backend.status().running()) {
            return this.status(request, Outcome.REJECTED, "source backend must remain stopped during relay");
        }
        final OfflineWorldTransactionPreparer.PreparedTransaction prepared = this.sourcePrepared(manifest);
        if (prepared == null) {
            return this.status(request, Outcome.REJECTED, "source shard tree has not been prepared");
        }
        final Path targetHalf = manifest.targetNodeId().equals(manifest.negativeNodeId())
            ? prepared.negativeHalf()
            : prepared.positiveHalf();
        final ResumableFileSender.TransferSummary summary = this.fileSender.sendTree(
            manifest.transactionId(),
            targetHalf,
            manifest.targetNodeId()
        );
        return this.status(
            request,
            Outcome.SUCCESS,
            "relayed " + summary.contentFiles() + " manifested files and " + summary.contentBytes()
                + " content bytes to the target"
        );
    }

    private Response installTarget(final Request request) throws IOException {
        this.requireAuthorization(request);
        final Manifest manifest = request.signedManifest().manifest();
        if (!this.nodeId.equals(manifest.targetNodeId())) {
            return this.status(request, Outcome.REJECTED, "only the target node may install relayed world data");
        }
        if (this.backend.status().running()) {
            return this.status(request, Outcome.REJECTED, "target backend must remain stopped during installation");
        }
        final OfflineTargetTransactionPreparer.PreparedTarget prepared = this.targetPrepared(manifest);
        if (prepared == null) {
            return this.status(request, Outcome.REJECTED, "target rollback point has not been prepared");
        }
        this.recoverInstalled(manifest, false);
        if (this.installed.containsKey(manifest.transactionId())) {
            return this.status(request, Outcome.SUCCESS, "target shard is already installed");
        }
        final Path staged = this.stagingRoot.resolve(
            Path.of("transactions", manifest.transactionId().toString(), "world")
        ).toAbsolutePath().normalize();
        if (!staged.startsWith(this.stagingRoot)) {
            throw new IOException("Target staging path escapes the configured staging root");
        }
        TransferTreeManifest.verify(staged);
        final Path transactionDirectory = prepared.journal().getParent();
        final WorldInstallationEngine.InstalledWorld result = WorldInstallationEngine.install(
            this.resolveWorld(manifest.worldDirectory()),
            staged,
            transactionDirectory,
            "target",
            shardManifest(manifest, false)
        );
        final WorldTransactionJournal journal = WorldTransactionJournal.load(prepared.journal());
        journal.advance(TransactionPhase.TARGET_PREPARED);
        this.installed.put(manifest.transactionId(), new InstalledRecord(
            result,
            prepared.worldInitiallyAbsent(),
            prepared.journal(),
            "target"
        ));
        return this.status(request, Outcome.SUCCESS, "target shard verified and atomically installed");
    }

    private Response commitSource(final Request request) throws IOException {
        this.requireAuthorization(request);
        final Manifest manifest = request.signedManifest().manifest();
        if (!this.nodeId.equals(manifest.sourceNodeId())) {
            return this.status(request, Outcome.REJECTED, "only the source node may commit source deletion");
        }
        if (this.backend.status().running()) {
            return this.status(request, Outcome.REJECTED, "source backend must remain stopped during commit");
        }
        final OfflineWorldTransactionPreparer.PreparedTransaction prepared = this.sourcePrepared(manifest);
        if (prepared == null) {
            return this.status(request, Outcome.REJECTED, "source shard tree has not been prepared");
        }
        this.recoverInstalled(manifest, true);
        if (this.installed.containsKey(manifest.transactionId())) {
            return this.status(request, Outcome.SUCCESS, "source shard is already committed");
        }
        final Path localHalf = this.nodeId.equals(manifest.negativeNodeId())
            ? prepared.negativeHalf()
            : prepared.positiveHalf();
        TransferTreeManifest.write(localHalf);
        TransferTreeManifest.verify(localHalf);
        final WorldTransactionJournal journal = WorldTransactionJournal.load(prepared.journal());
        if (journal.phase() == TransactionPhase.SPLIT_COMPLETE) {
            journal.advance(TransactionPhase.TARGET_PREPARED);
        } else if (journal.phase() != TransactionPhase.TARGET_PREPARED) {
            throw new IOException("Source journal cannot commit from phase " + journal.phase());
        }
        final WorldInstallationEngine.InstalledWorld result = WorldInstallationEngine.install(
            this.resolveWorld(manifest.worldDirectory()),
            localHalf,
            prepared.journal().getParent(),
            "source",
            shardManifest(manifest, true)
        );
        journal.advance(TransactionPhase.SOURCE_COMMITTED);
        this.installed.put(manifest.transactionId(), new InstalledRecord(
            result,
            false,
            prepared.journal(),
            "source"
        ));
        return this.status(
            request,
            Outcome.SUCCESS,
            "source shard atomically committed after target acknowledgement"
        );
    }

    private Response rollback(final Request request) throws IOException {
        final Manifest manifest = request.signedManifest().manifest();
        if (!this.hasAuthorization(request)) {
            final UUID active = this.activeTransaction.get();
            if (active != null && !active.equals(manifest.transactionId())) {
                return this.status(request, Outcome.REJECTED, "another world transaction is active");
            }
            return this.status(request, Outcome.SUCCESS, "no local transaction state required rollback");
        }
        this.claim(manifest.transactionId());
        this.recoverInstalled(manifest, this.nodeId.equals(manifest.sourceNodeId()));
        final InstalledRecord record = this.installed.remove(manifest.transactionId());
        Path journalPath = null;
        if (record != null) {
            final Path failed = record.journal().getParent().resolve(record.role() + "-failed");
            WorldInstallationEngine.rollback(
                this.resolveWorld(manifest.worldDirectory()),
                record.world().retiredOriginal(),
                record.worldInitiallyAbsent(),
                failed
            );
            journalPath = record.journal();
        } else if (this.nodeId.equals(manifest.sourceNodeId())) {
            final var source = this.prepared.get(manifest.transactionId());
            journalPath = source == null ? null : source.journal();
        } else {
            final var target = this.preparedTargets.get(manifest.transactionId());
            journalPath = target == null ? null : target.journal();
        }
        if (journalPath != null) {
            final WorldTransactionJournal journal = WorldTransactionJournal.load(journalPath);
            if (journal.phase() != TransactionPhase.ROLLED_BACK) {
                journal.advance(TransactionPhase.ROLLED_BACK);
            }
        }
        return this.status(request, Outcome.SUCCESS, "local transaction state rolled back; diagnostics retained");
    }

    private Response complete(final Request request) throws IOException {
        final Manifest manifest = request.signedManifest().manifest();
        if (!this.hasAuthorization(request)) {
            final UUID active = this.activeTransaction.get();
            if (active != null && !active.equals(manifest.transactionId())) {
                return this.status(request, Outcome.REJECTED, "another world transaction is active");
            }
            this.activeTransaction.compareAndSet(manifest.transactionId(), null);
            return this.status(request, Outcome.SUCCESS, "no local transaction authorization required completion");
        }
        this.claim(manifest.transactionId());
        if (!this.backend.status().running()) {
            return this.status(request, Outcome.REJECTED, "backend must be running before transaction completion");
        }
        final Path journalPath;
        if (this.nodeId.equals(manifest.sourceNodeId())) {
            final OfflineWorldTransactionPreparer.PreparedTransaction source = this.sourcePrepared(manifest);
            journalPath = source == null ? null : source.journal();
        } else {
            final OfflineTargetTransactionPreparer.PreparedTarget target = this.targetPrepared(manifest);
            journalPath = target == null ? null : target.journal();
        }
        if (journalPath == null) {
            this.clearAuthorization(manifest.transactionId());
            return this.status(request, Outcome.SUCCESS, "authorization-only transaction completed");
        }
        final WorldTransactionJournal journal = WorldTransactionJournal.load(journalPath);
        if (journal.phase() == TransactionPhase.ROLLED_BACK) {
            this.clearAuthorization(manifest.transactionId());
            return this.status(request, Outcome.SUCCESS, "rolled-back transaction completed");
        }
        if (journal.phase() == TransactionPhase.STARTING_TARGET) {
            journal.advance(TransactionPhase.STARTING_SOURCE);
        }
        if (journal.phase() == TransactionPhase.STARTING_SOURCE) {
            journal.advance(TransactionPhase.COMPLETE);
        }
        if (journal.phase() != TransactionPhase.COMPLETE) {
            throw new IOException("local transaction journal cannot complete from " + journal.phase());
        }
        this.clearAuthorization(manifest.transactionId());
        return this.status(request, Outcome.SUCCESS, "local transaction journal completed");
    }

    private void clearAuthorization(final UUID transactionId) throws IOException {
        this.authorized.remove(transactionId);
        this.activeTransaction.compareAndSet(transactionId, null);
        final Path path = this.transactionRoot.resolve(transactionId.toString())
            .resolve("authorization.sha256").normalize();
        if (!path.startsWith(this.transactionRoot)) {
            throw new IOException("Authorization cleanup path escapes the transaction root");
        }
        Files.deleteIfExists(path);
    }

    private ShardManifestWriter.Manifest shardManifest(final Manifest manifest, final boolean source) {
        final boolean negative = this.nodeId.equals(manifest.negativeNodeId());
        return new ShardManifestWriter.Manifest(
            manifest.worldKey(),
            manifest.worldId(),
            manifest.transactionId(),
            manifest.axis() == WorldTransactionCodec.Axis.X ? ShardAxis.X : ShardAxis.Z,
            manifest.cutChunk(),
            negative ? ShardSide.NEGATIVE : ShardSide.POSITIVE,
            source ? manifest.targetBackendId() : manifest.sourceBackendId()
        );
    }

    private ProtocolFrame handleLocalRequest(final ProtocolFrame request) throws IOException {
        if (request.channel() != ProtocolChannel.WORLD_TRANSACTION
            || request.messageType() != MessageType.WORLD_TRANSACTION_RESPONSE) {
            throw new IOException("unsupported local transaction response");
        }
        final Response response = WorldTransactionCodec.decodeResponse(request.payload());
        final CompletableFuture<Response> pending = this.backendAuthorizations.get(response.transactionId());
        if (pending == null) {
            throw new IOException("no matching backend transaction authorization is pending");
        }
        pending.complete(response);
        return new ProtocolFrame(
            ShardingbaseProtocol.VERSION,
            ProtocolChannel.WORLD_TRANSACTION,
            MessageType.BACKEND_SEND_ACK,
            request.correlationId(),
            "node",
            request.sourceId(),
            new byte[0]
        );
    }

    private void requireAuthorization(final Request request) throws IOException {
        if (!this.hasAuthorization(request)) {
            throw new IOException("proxy and local backend have not authorized this exact transaction manifest");
        }
        this.claim(request.signedManifest().manifest().transactionId());
    }

    private boolean hasAuthorization(final Request request) throws IOException {
        final Manifest manifest = request.signedManifest().manifest();
        byte[] expected = this.authorized.get(manifest.transactionId());
        if (expected == null) {
            expected = this.loadAuthorization(manifest.transactionId());
            if (expected != null) {
                this.authorized.put(manifest.transactionId(), expected);
            }
        }
        final byte[] actual = WorldTransactionCodec.digest(manifest);
        return expected != null && Arrays.equals(expected, actual);
    }

    private void persistAuthorization(final UUID transactionId, final byte[] digest) throws IOException {
        final Path directory = this.transactionRoot.resolve(transactionId.toString()).normalize();
        if (!directory.startsWith(this.transactionRoot)) {
            throw new IOException("Authorization path escapes the transaction root");
        }
        Files.createDirectories(directory);
        final Path target = directory.resolve("authorization.sha256");
        final Path temporary = Files.createTempFile(directory, ".authorization-", ".tmp");
        try {
            Files.writeString(temporary, HexFormat.of().formatHex(digest) + '\n', StandardCharsets.US_ASCII);
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (final AtomicMoveNotSupportedException exception) {
                throw new IOException("Atomic transaction authorization persistence is not supported", exception);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private byte[] loadAuthorization(final UUID transactionId) throws IOException {
        final Path path = this.transactionRoot.resolve(transactionId.toString())
            .resolve("authorization.sha256").normalize();
        if (!path.startsWith(this.transactionRoot) || Files.notExists(path)) {
            return null;
        }
        final String value = Files.readString(path, StandardCharsets.US_ASCII).strip();
        if (!value.matches("[0-9a-f]{64}")) {
            throw new IOException("Persisted transaction authorization digest is invalid");
        }
        return HexFormat.of().parseHex(value);
    }

    private OfflineWorldTransactionPreparer.PreparedTransaction sourcePrepared(final Manifest manifest)
        throws IOException {
        final OfflineWorldTransactionPreparer.PreparedTransaction existing =
            this.prepared.get(manifest.transactionId());
        if (existing != null) {
            return existing;
        }
        final Path directory = this.transactionRoot.resolve(manifest.transactionId().toString());
        final Path journalPath = directory.resolve("journal.properties");
        if (Files.notExists(journalPath)) {
            return null;
        }
        final TransactionPhase phase = WorldTransactionJournal.load(journalPath).phase();
        if (phase != TransactionPhase.SPLIT_COMPLETE
            && phase != TransactionPhase.TARGET_PREPARED
            && phase != TransactionPhase.SOURCE_COMMITTED
            && phase != TransactionPhase.STARTING_TARGET
            && phase != TransactionPhase.STARTING_SOURCE
            && phase != TransactionPhase.COMPLETE
            && phase != TransactionPhase.ROLLED_BACK) {
            return null;
        }
        final Path backup = this.backupRoot.resolve(manifest.transactionId().toString());
        final OfflineWorldTransactionPreparer.PreparedTransaction recovered =
            new OfflineWorldTransactionPreparer.PreparedTransaction(
                journalPath,
                new dev.shardingbase.node.world.WorldBackupEngine.BackupResult(backup, 0L, 0L),
                directory.resolve("negative"),
                directory.resolve("positive"),
                new dev.shardingbase.node.world.WorldSplitEngine.SplitSummary(0, 0, 0)
            );
        this.prepared.put(manifest.transactionId(), recovered);
        return recovered;
    }

    private OfflineTargetTransactionPreparer.PreparedTarget targetPrepared(final Manifest manifest)
        throws IOException {
        final OfflineTargetTransactionPreparer.PreparedTarget existing =
            this.preparedTargets.get(manifest.transactionId());
        if (existing != null) {
            return existing;
        }
        final Path directory = this.transactionRoot.resolve(manifest.transactionId().toString());
        final Path journalPath = directory.resolve("journal.properties");
        if (Files.notExists(journalPath)) {
            return null;
        }
        final TransactionPhase phase = WorldTransactionJournal.load(journalPath).phase();
        if (phase != TransactionPhase.BACKUP_COMPLETE
            && phase != TransactionPhase.TARGET_PREPARED
            && phase != TransactionPhase.SOURCE_COMMITTED
            && phase != TransactionPhase.STARTING_TARGET
            && phase != TransactionPhase.STARTING_SOURCE
            && phase != TransactionPhase.COMPLETE
            && phase != TransactionPhase.ROLLED_BACK) {
            return null;
        }
        final Path backup = this.backupRoot.resolve(manifest.transactionId().toString());
        final OfflineTargetTransactionPreparer.PreparedTarget recovered =
            new OfflineTargetTransactionPreparer.PreparedTarget(
                journalPath,
                new dev.shardingbase.node.world.WorldBackupEngine.BackupResult(backup, 0L, 0L),
                Files.isRegularFile(backup.resolve("world.absent"))
            );
        this.preparedTargets.put(manifest.transactionId(), recovered);
        return recovered;
    }

    private void recoverInstalled(final Manifest manifest, final boolean source) throws IOException {
        if (this.installed.containsKey(manifest.transactionId())) {
            return;
        }
        final Path world = this.resolveWorld(manifest.worldDirectory());
        final Path shardManifest = world.resolve(ShardManifestWriter.FILE_NAME);
        if (!Files.isRegularFile(shardManifest)) {
            return;
        }
        final Properties properties = new Properties();
        try (java.io.InputStream input = Files.newInputStream(shardManifest)) {
            properties.load(input);
        }
        if (!manifest.transactionId().toString().equals(properties.getProperty("transaction-id"))) {
            return;
        }
        final Path journal = this.transactionRoot.resolve(manifest.transactionId().toString())
            .resolve("journal.properties");
        final Path retired = journal.getParent().resolve((source ? "source" : "target") + "-original");
        final boolean absent = !source
            && Files.isRegularFile(this.backupRoot.resolve(manifest.transactionId().toString()).resolve("world.absent"));
        this.installed.put(manifest.transactionId(), new InstalledRecord(
            new WorldInstallationEngine.InstalledWorld(world, Files.exists(retired) ? retired : null),
            absent,
            journal,
            source ? "source" : "target"
        ));
    }

    private void claim(final UUID transactionId) throws IOException {
        final UUID active = this.activeTransaction.get();
        if (active == null) {
            if (!this.activeTransaction.compareAndSet(null, transactionId)) {
                this.claim(transactionId);
            }
            return;
        }
        if (!active.equals(transactionId)) {
            throw new IOException("another world transaction is already active on this node");
        }
    }

    private Response status(final Request request, final Outcome outcome, final String detail) throws IOException {
        final BackendProcess.Status status = this.backend.status();
        return new Response(
            request.signedManifest().manifest().transactionId(),
            request.operation(),
            outcome,
            detail,
            status.running(),
            status.processId() == null ? -1L : status.processId(),
            status.lastExitCode() == null ? -1 : status.lastExitCode(),
            usableBytes(nearestExisting(this.backupRoot)),
            WorldTransactionCodec.digest(request.signedManifest().manifest())
        );
    }

    private void respondFailure(
        final ProtocolFrame frame,
        final UUID transactionId,
        final Operation operation,
        final String detail
    ) {
        try {
            final BackendProcess.Status status = this.backend.status();
            final byte[] digest = new byte[32];
            this.proxy.respond(frame, MessageType.WORLD_TRANSACTION_RESPONSE, WorldTransactionCodec.encodeResponse(
                new Response(
                    transactionId == null ? frame.correlationId() : transactionId,
                    operation,
                    Outcome.FAILED,
                    detail,
                    status.running(),
                    status.processId() == null ? -1L : status.processId(),
                    status.lastExitCode() == null ? -1 : status.lastExitCode(),
                    -1L,
                    digest
                )
            ));
        } catch (final IOException ignored) {
        }
    }

    private Path resolveWorld(final String relative) throws IOException {
        final Path path = this.worldRoot.resolve(relative).toAbsolutePath().normalize();
        if (!path.startsWith(this.worldRoot)) {
            throw new IOException("World path escapes the configured world root");
        }
        return path;
    }

    private static long directoryBytes(final Path root) throws IOException {
        long total = 0L;
        try (var paths = Files.walk(root)) {
            final var iterator = paths.iterator();
            while (iterator.hasNext()) {
                final Path path = iterator.next();
                if (Files.isRegularFile(path)) {
                    total = saturatedAdd(total, Files.size(path));
                }
            }
        }
        return total;
    }

    static long safetyMargin(final long bytes) {
        if (bytes > Long.MAX_VALUE / 6L * 5L) {
            return Long.MAX_VALUE;
        }
        return bytes + (bytes + 4L) / 5L;
    }

    private static long saturatedAdd(final long left, final long right) {
        if (right > Long.MAX_VALUE - left) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long usableBytes(final Path path) throws IOException {
        final FileStore store = Files.getFileStore(path);
        return store.getUsableSpace();
    }

    private static Path nearestExisting(final Path path) throws IOException {
        Path current = path;
        while (current != null && Files.notExists(current)) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IOException("No existing parent exists for configured path " + path);
        }
        return current;
    }

    private static Path configuredPath(final String configured, final Path base, final Path fallback) {
        if (configured == null || configured.isBlank()) {
            return fallback.toAbsolutePath().normalize();
        }
        final Path path = Path.of(configured);
        return (path.isAbsolute() ? path : base.resolve(path)).toAbsolutePath().normalize();
    }

    static byte[] signingKey(final String encoded) throws IOException {
        try {
            final byte[] key = Base64.getUrlDecoder().decode(encoded);
            if (key.length < 32) {
                throw new IOException(SIGNING_KEY_ENVIRONMENT + " must decode to at least 32 bytes");
            }
            return key;
        } catch (final IllegalArgumentException exception) {
            throw new IOException(SIGNING_KEY_ENVIRONMENT + " must be URL-safe base64", exception);
        }
    }

    private static String safeMessage(final Exception exception) {
        final String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        this.worker.shutdownNow();
        this.backendAuthorizations.forEach((id, future) ->
            future.completeExceptionally(new IOException("Node transaction controller is shutting down")));
        this.backendAuthorizations.clear();
    }

    private record InstalledRecord(
        WorldInstallationEngine.InstalledWorld world,
        boolean worldInitiallyAbsent,
        Path journal,
        String role
    ) {
    }
}
