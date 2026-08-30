package dev.shardingbase.velocity;

import com.google.inject.Inject;
import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import com.velocitypowered.api.event.EventTask;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.command.CommandExecuteEvent;
import com.velocitypowered.api.event.command.PlayerAvailableCommandsEvent;
import com.velocitypowered.api.event.player.KickedFromServerEvent;
import com.velocitypowered.api.event.player.ServerPreConnectEvent;
import com.velocitypowered.api.event.player.TabCompleteEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.ProxyServer;
import dev.shardingbase.protocol.RemoteCommandCodec;
import dev.shardingbase.protocol.ShardingbaseProtocol;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;

@Plugin(
    id = "shardingbase",
    name = "Shardingbase",
    version = "0.1.0-SNAPSHOT",
    description = "Velocity controller for Shardingbase backends"
)
public final class ShardingbaseVelocity {
    private final ProxyServer proxy;
    private final Logger logger;
    private final Path dataDirectory;
    private volatile ControlServer controlServer;
    private volatile PlayerTransferCoordinator playerTransfers;
    private volatile BackendRegistry backendRegistry;
    private volatile Set<String> remoteCommandAllowlist = Set.of();
    private volatile PlannerWebServer plannerWebServer;
    private volatile VelocityWorldTransactionCoordinator worldTransactions;

    @Inject
    public ShardingbaseVelocity(
        final ProxyServer proxy,
        final Logger logger,
        final @DataDirectory Path dataDirectory
    ) {
        this.proxy = proxy;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public EventTask onProxyInitialization(final ProxyInitializeEvent event) {
        return EventTask.async(this::initialize);
    }

    @Subscribe
    public void onProxyShutdown(final ProxyShutdownEvent event) {
        final PlannerWebServer planner = this.plannerWebServer;
        if (planner != null) {
            planner.close();
        }
        final VelocityWorldTransactionCoordinator transactions = this.worldTransactions;
        if (transactions != null) {
            transactions.close();
        }
        final ControlServer current = this.controlServer;
        if (current != null) {
            try {
                current.close();
            } catch (final IOException exception) {
                this.logger.warn("Unable to close the Shardingbase control listener cleanly", exception);
            }
        }
    }

    @Subscribe
    public EventTask onServerPreConnect(final ServerPreConnectEvent event) {
        final PlayerTransferCoordinator current = this.playerTransfers;
        return current == null ? null : current.beforeConnect(event);
    }

    @Subscribe
    public EventTask onKickedFromServer(final KickedFromServerEvent event) {
        final PlayerTransferCoordinator current = this.playerTransfers;
        return current == null ? null : current.afterSourceKick(event);
    }

    @Subscribe
    public void onDisconnect(final DisconnectEvent event) {
        final PlayerTransferCoordinator current = this.playerTransfers;
        if (current != null) {
            current.disconnected(event.getPlayer().getUniqueId());
        }
    }

    @Subscribe
    public EventTask onCommandExecute(final CommandExecuteEvent event) {
        if (!(event.getCommandSource() instanceof final com.velocitypowered.api.proxy.Player player)) {
            return null;
        }
        final Optional<BackendRegistry.BackendTarget> route = this.remoteRoute(player, event.getCommand());
        if (route.isEmpty()) {
            return null;
        }
        final ControlServer control = this.controlServer;
        if (control == null) {
            return null;
        }
        event.setResult(CommandExecuteEvent.CommandResult.denied());
        final var operation = control.command(
            route.orElseThrow(), RemoteCommandCodec.Operation.EXECUTE, stripSlash(event.getCommand())
        ).whenComplete((response, failure) -> {
            if (failure != null) {
                player.sendMessage(Component.text("Remote command failed: " + failureMessage(failure)));
                return;
            }
            if (response.outcome() != RemoteCommandCodec.Outcome.SUCCESS || response.lines().isEmpty()) {
                player.sendMessage(Component.text(response.detail()));
            }
            response.lines().forEach(line -> player.sendMessage(Component.text(line)));
        });
        return EventTask.resumeWhenComplete(operation);
    }

    @Subscribe
    public void onPlayerAvailableCommands(final PlayerAvailableCommandsEvent event) {
        final ControlServer control = this.controlServer;
        final BackendRegistry registry = this.backendRegistry;
        final String currentName = currentServerName(event.getPlayer()).orElse(null);
        if (control == null || registry == null || currentName == null) {
            return;
        }
        try {
            final BackendRegistry.BackendTarget current = registry.backendForName(currentName).orElse(null);
            final BackendRegistry.BackendTarget peer = registry.peerForName(currentName).orElse(null);
            if (current == null || peer == null) {
                return;
            }
            for (final String label : this.remoteCommandAllowlist) {
                if (!control.commandCatalog(current.serverId()).contains(label)
                    && control.commandCatalog(peer.serverId()).contains(label)) {
                    addRemoteCommand(event, label);
                }
            }
        } catch (final IOException exception) {
            this.logger.warn("Unable to build the Shardingbase remote command tree", exception);
        }
    }

    @Subscribe
    public EventTask onTabComplete(final TabCompleteEvent event) {
        final Optional<BackendRegistry.BackendTarget> route = this.remoteRoute(
            event.getPlayer(), event.getPartialMessage()
        );
        final ControlServer control = this.controlServer;
        if (route.isEmpty() || control == null) {
            return null;
        }
        final var operation = control.command(
            route.orElseThrow(), RemoteCommandCodec.Operation.SUGGEST, stripSlash(event.getPartialMessage())
        ).thenAccept(response -> {
            if (response.outcome() == RemoteCommandCodec.Outcome.SUCCESS) {
                event.getSuggestions().addAll(response.lines());
            }
        }).exceptionally(failure -> null);
        return EventTask.resumeWhenComplete(operation);
    }

    private void initialize() {
        try {
            final VelocityConfiguration configuration = VelocityConfiguration.load(this.dataDirectory);
            final TlsMaterial tlsMaterial = TlsMaterial.loadOrCreate(configuration);
            final BackendRegistry registry = new BackendRegistry(configuration.databasePath());
            final PlayerStateStore playerStateStore = new PlayerStateStore(configuration.databasePath());
            final WorldPlannerStore worldPlannerStore = new WorldPlannerStore(configuration.databasePath());
            this.controlServer = new ControlServer(
                this.proxy, this.logger, configuration, tlsMaterial, registry, playerStateStore, worldPlannerStore
            );
            this.worldTransactions = new VelocityWorldTransactionCoordinator(
                configuration, worldPlannerStore, registry, this.controlServer, this.logger
            );
            this.plannerWebServer = new PlannerWebServer(
                configuration, worldPlannerStore, registry, this.worldTransactions::submit, this.logger
            );
            this.backendRegistry = registry;
            this.remoteCommandAllowlist = configuration.remoteCommandAllowlist();
            this.playerTransfers = new PlayerTransferCoordinator(
                registry,
                playerStateStore,
                this.controlServer,
                this.logger
            );
            this.logger.info(
                "Shardingbase controller listening on {}:{} for {} registered Velocity backend(s); protocol {}; TLS SHA-256 {}",
                configuration.bindAddress(),
                configuration.controlPort(),
                this.proxy.getAllServers().size(),
                ShardingbaseProtocol.VERSION,
                tlsMaterial.fingerprint()
            );
        } catch (final Exception exception) {
            this.logger.error("Shardingbase controller failed to initialize; distributed features are unavailable", exception);
        }
    }

    private Optional<BackendRegistry.BackendTarget> remoteRoute(
        final com.velocitypowered.api.proxy.Player player,
        final String commandLine
    ) {
        final ControlServer control = this.controlServer;
        final BackendRegistry registry = this.backendRegistry;
        final String currentName = currentServerName(player).orElse(null);
        final String command = stripSlash(commandLine);
        final String root = command.substring(0, command.indexOf(' ') < 0 ? command.length() : command.indexOf(' '))
            .toLowerCase(Locale.ROOT);
        if (control == null || registry == null || currentName == null || root.isBlank()
            || !this.remoteCommandAllowlist.contains(root)) {
            return Optional.empty();
        }
        try {
            final BackendRegistry.BackendTarget current = registry.backendForName(currentName).orElse(null);
            final BackendRegistry.BackendTarget peer = registry.peerForName(currentName).orElse(null);
            if (current == null || peer == null || control.commandCatalog(current.serverId()).contains(root)
                || !control.commandCatalog(peer.serverId()).contains(root)) {
                return Optional.empty();
            }
            return Optional.of(peer);
        } catch (final IOException exception) {
            this.logger.warn("Unable to route remote command /{}", root, exception);
            return Optional.empty();
        }
    }

    private static Optional<String> currentServerName(final com.velocitypowered.api.proxy.Player player) {
        return player.getCurrentServer().map(connection -> connection.getServerInfo().getName());
    }

    private static String stripSlash(final String commandLine) {
        final String stripped = commandLine.startsWith("/") ? commandLine.substring(1) : commandLine;
        return stripped.strip();
    }

    private static String failureMessage(final Throwable failure) {
        final Throwable cause = failure.getCause() == null ? failure : failure.getCause();
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void addRemoteCommand(final PlayerAvailableCommandsEvent event, final String label) {
        final com.mojang.brigadier.tree.RootCommandNode root = event.getRootNode();
        root.addChild(LiteralArgumentBuilder.literal(label).build());
    }
}
