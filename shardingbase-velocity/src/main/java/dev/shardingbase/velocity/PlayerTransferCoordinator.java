package dev.shardingbase.velocity;

import com.velocitypowered.api.event.EventTask;
import com.velocitypowered.api.event.player.KickedFromServerEvent;
import com.velocitypowered.api.event.player.ServerPreConnectEvent;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import dev.shardingbase.protocol.PlayerDataCategory;
import dev.shardingbase.protocol.PlayerHandoffCodec;
import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;

/** Pauses managed switches until post-quit state is durably staged for the target. */
final class PlayerTransferCoordinator {
    private static final Duration STAGE_TIMEOUT = Duration.ofSeconds(10);

    private final BackendRegistry registry;
    private final PlayerStateStore playerStateStore;
    private final ControlServer controlServer;
    private final Logger logger;
    private final ConcurrentHashMap<UUID, PendingTransfer> pending = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, String> bypass = new ConcurrentHashMap<>();

    PlayerTransferCoordinator(
        final BackendRegistry registry,
        final PlayerStateStore playerStateStore,
        final ControlServer controlServer,
        final Logger logger
    ) {
        this.registry = registry;
        this.playerStateStore = playerStateStore;
        this.controlServer = controlServer;
        this.logger = logger;
    }

    EventTask beforeConnect(final ServerPreConnectEvent event) {
        if (!event.getResult().isAllowed()) {
            return null;
        }
        final RegisteredServer target = event.getResult().getServer().orElse(event.getOriginalServer());
        final RegisteredServer source = event.getPreviousServer();
        if (source == null || source.getServerInfo().getName().equals(target.getServerInfo().getName())) {
            return null;
        }
        final UUID playerId = event.getPlayer().getUniqueId();
        if (target.getServerInfo().getName().equals(this.bypass.remove(playerId))) {
            return null;
        }
        event.setResult(ServerPreConnectEvent.ServerResult.denied());
        return EventTask.async(() -> this.begin(event.getPlayer(), source, target));
    }

    EventTask afterSourceKick(final KickedFromServerEvent event) {
        final PendingTransfer transfer = this.pending.get(event.getPlayer().getUniqueId());
        if (transfer == null
            || !transfer.source().getServerInfo().getName().equals(event.getServer().getServerInfo().getName())) {
            return null;
        }
        return EventTask.async(() -> this.finish(event, transfer));
    }

    void disconnected(final UUID playerId) {
        this.pending.remove(playerId);
        this.bypass.remove(playerId);
    }

    private void begin(final Player player, final RegisteredServer sourceServer, final RegisteredServer targetServer) {
        try {
            final BackendRegistry.BackendTarget source = this.registry
                .backendForName(sourceServer.getServerInfo().getName())
                .orElseThrow(() -> new IOException("source backend is not registered"));
            final BackendRegistry.BackendTarget target = this.registry
                .backendForName(targetServer.getServerInfo().getName())
                .orElseThrow(() -> new IOException("target backend is not registered"));
            final long revision = this.playerStateStore.reserveRevision(player.getUniqueId());
            final var categories = this.playerStateStore.categories();
            final PendingTransfer transfer = new PendingTransfer(sourceServer, targetServer, target.serverId(), revision);
            if (this.pending.putIfAbsent(player.getUniqueId(), transfer) != null) {
                throw new IOException("a player handoff is already active");
            }
            this.controlServer.sendPlayerCapture(source, new PlayerHandoffCodec.Capture(
                player.getUniqueId(),
                target.serverId(),
                revision,
                categories
            ));
        } catch (final IOException exception) {
            this.pending.remove(player.getUniqueId());
            player.sendMessage(Component.text("Shardingbase transfer could not start: " + exception.getMessage()));
            this.logger.warn("Unable to start managed player handoff for {}", player.getUniqueId(), exception);
        }
    }

    private void finish(final KickedFromServerEvent event, final PendingTransfer transfer) {
        final UUID playerId = event.getPlayer().getUniqueId();
        try {
            if (!this.playerStateStore.awaitStage(
                playerId,
                transfer.revision(),
                transfer.targetBackendId(),
                STAGE_TIMEOUT.toMillis()
            )) {
                throw new IOException("timed out waiting for the source snapshot");
            }
            this.bypass.put(playerId, transfer.target().getServerInfo().getName());
            event.setResult(KickedFromServerEvent.RedirectPlayer.create(
                transfer.target(),
                Component.text("Transferring to the peer shard…")
            ));
        } catch (final IOException exception) {
            event.setResult(KickedFromServerEvent.DisconnectPlayer.create(
                Component.text("Shardingbase transfer failed safely: " + exception.getMessage())
            ));
            this.logger.warn("Managed player handoff failed for {}", playerId, exception);
        } finally {
            this.pending.remove(playerId, transfer);
        }
    }

    private record PendingTransfer(
        RegisteredServer source,
        RegisteredServer target,
        String targetBackendId,
        long revision
    ) {
    }
}
