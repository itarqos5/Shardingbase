package dev.shardingbase.node;

import dev.shardingbase.node.world.OfflineWorldTransactionPreparer;
import dev.shardingbase.node.world.ShardAxis;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
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
    private final String nodeId;
    private final byte[] signingKey;
    private final String unavailableDetail;
    private final Path worldRoot;
    private final Path backupRoot;
    private final Path transactionRoot;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(task -> Thread.ofPlatform()
        .daemon(true)
        .name("Shardingbase World Transaction")
        .unstarted(task));
    private final ConcurrentHashMap<UUID, CompletableFuture<Response>> backendAuthorizations =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, byte[]> authorized = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, OfflineWorldTransactionPreparer.PreparedTransaction> prepared =
        new ConcurrentHashMap<>();
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
        if (this.prepared.containsKey(manifest.transactionId())) {
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
        return this.status(request, Outcome.SUCCESS, "backend restart launched");
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
        final Manifest manifest = request.signedManifest().manifest();
        final byte[] expected = this.authorized.get(manifest.transactionId());
        final byte[] actual = WorldTransactionCodec.digest(manifest);
        if (expected == null || !Arrays.equals(expected, actual)) {
            throw new IOException("proxy and local backend have not authorized this exact transaction manifest");
        }
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
}
