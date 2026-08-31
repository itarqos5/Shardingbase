# Shardingbase

[![Build status](https://img.shields.io/github/actions/workflow/status/itarqos5/Shardingbase/build.yml?branch=feature%2Fshardingbase-prototype&label=build)](https://github.com/itarqos5/Shardingbase/actions)
[![Upstream](https://img.shields.io/badge/upstream-Paper-344ceb)](https://github.com/PaperMC/Paper)

Shardingbase is an experimental Paper 26.2 fork for exactly two backend servers behind one Velocity proxy. It preserves ordinary Paper behavior on each backend and adds authenticated coordination, portable player-state transfer, coordinate-preserving shard crossing, remote operations, map planning, and an offline world-cut transaction.

This branch is a working prototype, not a production storage system. Always keep an independent world backup and test the complete cut, rollback, and plugin set before using real player data.

## Paper plugin compatibility

Shardingbase retains the Bukkit, Spigot, Paper, and legacy plugin loaders and the `org.bukkit` and `io.papermc.paper` APIs. It advertises the exact upstream Paper 26.2 build 121 API identity (`26.2.build.121-stable`) and Paper compatibility build/commit data to plugins that perform version checks. The visible server brand remains Shardingbase; compatibility calls which incorrectly gate on `Bukkit.getName()` receive `Paper`.

No fork can guarantee every third-party plugin. Shardingbase avoids fake remote Bukkit objects: a plugin still sees the worlds, chunks, entities, and players loaded by its current backend. Normal local plugin behavior remains Paper-compatible, while explicit remote work uses `dev.shardingbase.api`.

The included runtime matrix currently starts and enables these real public builds together:

- LuckPerms, HuskHomes, EssentialsX, PlaceholderAPI, and VaultUnlocked;
- ViaVersion and ViaBackwards;
- Multiverse-Core;
- WorldEdit with WorldGuard;
- FastAsyncWorldEdit with WorldGuard.

The public CoreProtect Community Edition artifact is also exercised, but is recorded as an expected upstream incompatibility because that public build rejects Minecraft 26.2. The matrix does not hide its disable message or label it a Shardingbase pass.

FastAsyncWorldEdit is detected by JAR content, including renamed JARs. Only when FAWE is installed does the supervisor select FAWE's Paper 26.2 adapter. Plain WorldEdit uses its normal Paperweight adapter.

## Global coordinates and homes

Shards do not rebase coordinates. Cuts are aligned to chunk boundaries, and the world retains one absolute coordinate system on both servers. For example, a requested split near X `25,000` uses the aligned cut at X `24,992`:

- shard A owns blocks through `24,991`;
- shard B begins at block `24,992`;
- a player crossing arrives on shard B at the same X/Y/Z, yaw, pitch, world key, and world UUID;
- plugins on shard B read X `25,000`, not X `0`.

Ordinary Paper `Player#teleport` and `teleportAsync` calls are routed through the managed Velocity handoff when their final `PlayerTeleportEvent` destination belongs to the peer. The target is staged before spawn-chunk loading; portable state is applied before join packets/events, and an unsafe exact block is adjusted only within a bounded four-block search.

A homes plugin must still make the saved home visible to both servers. For HuskHomes, install the same version on both backends and configure both copies with:

```yaml
database:
  type: MARIADB # MYSQL or POSTGRESQL also works
  credentials:
    host: database.internal
    port: 3306
    database: huskhomes
    username: huskhomes
    password: replace-me

cross_server:
  enabled: true
  cluster_id: main
  broker_type: PLUGIN_MESSAGE # or REDIS
```

Both backends must use the same database, cluster ID, and broker configuration. Keep the HuskHomes server identity/Velocity configuration distinct as required by HuskHomes. With that setup, the plugin can find a home saved on the other shard and Shardingbase preserves its absolute destination coordinates. Other homes plugins need an equivalent shared database or supported cross-server mode; two independent SQLite files cannot share home records.

## Build artifacts

Build with JDK 25:

```powershell
.\gradlew.bat assembleShardingbaseRelease
```

Only two distributable files are written to `build/release/`:

- `server.jar` — the backend supervisor with the Paper-compatible Shardingbase backend embedded as a jar-in-jar;
- `shardingbase-velocity.jar` — the Velocity 4.1 controller plugin.

`server.jar` exports the embedded backend to `cache/backend.jar`. On every launch it byte-compares the embedded and cached copies, then atomically extracts or replaces a missing/outdated file. Uploading only `server.jar` is sufficient for a backend.

## Backend installation

Place `server.jar` in an otherwise normal Paper server directory:

```bash
java -Xms64M -Xmx128M -jar server.jar nogui
```

The parent heap belongs to the lightweight supervisor. Set `SHARDINGBASE_BACKEND_MEMORY_MB` for the child Minecraft heap; for example `4096` gives the backend `-Xms128M -Xmx4096M`. Minecraft arguments after the JAR are forwarded unchanged.

On first backend startup, `config/shardingbase.yml` is created:

```yaml
server-id: "generated-unique-id"
server-name: "velocity-server-name"
```

Use a unique non-empty `server-id` on each backend. `server-name` must exactly match one of Velocity's registered server names. A wrong but valid name leaves normal Paper online while distributed features stay disabled. Ambiguous/unsafe configuration fails clearly instead of being guessed.

The supervisor handles normal `stop`, panel termination, and Ctrl+C. A normal backend stop ends the supervisor so process managers see a clean shutdown. During an authorized world transaction, the supervisor remains alive while the child is stopped, performs the offline work, and accepts the coordinated restart.

## Velocity installation

Put `shardingbase-velocity.jar` in `velocity/plugins/` and start Velocity once. The plugin creates:

- `plugins/shardingbase/config.yml`;
- `plugins/shardingbase/tls.p12`;
- `plugins/shardingbase/shardingbase.db`.

The Velocity JAR shades its SQLite JDBC driver, so no separate database driver is required. A representative configuration is:

```yaml
control:
  bind: 0.0.0.0
  port: 8443
  keystore: tls.p12
  keystore-password: generated-secret
  transaction-signing-key: generated-secret
database: shardingbase.db
node-credentials:
  node-a: generated-secret
  node-b: generated-secret
remote-command-allowlist: []
web:
  bind: 0.0.0.0
  port: 8080
  tls-enabled: false
  public-url: https://proxy.example.net:8080
```

The control listener uses TLS 1.3, certificate pinning, per-node credentials, bounded frames/queues, replay checks, checksums, timeouts, and correlated responses. Put the planner behind a trusted HTTPS reverse proxy and set `web.tls-enabled: false` as above; `public-url` describes the browser-facing URL independently of the listener transport. Direct planner TLS is available with `tls-enabled: true`, but the automatically generated certificate is self-signed for localhost/`127.0.0.1`, so a public hostname needs an explicitly trusted replacement keystore. The control and planner listeners need separate reachable allocations from Velocity's Minecraft port because they are separate TCP listeners.

Configure each node with:

```text
SHARDINGBASE_CONTROLLER_URI=tls://proxy.example.net:8443
SHARDINGBASE_CERTIFICATE_SHA256=<fingerprint printed by Velocity>
SHARDINGBASE_NODE_ID=node-a
SHARDINGBASE_NODE_CREDENTIAL=<matching generated credential>
SHARDINGBASE_TRANSACTION_KEY=<matching transaction-signing-key>
SHARDINGBASE_BACKEND_MEMORY_MB=4096
SHARDINGBASE_WORLD_ROOT=.
SHARDINGBASE_BACKUP_ROOT=backups
SHARDINGBASE_TRANSACTION_ROOT=shardingbase-transactions
SHARDINGBASE_STAGING_ROOT=shardingbase-staging
```

Do not use `127.0.0.1` when Velocity is in another container; loopback points to the backend container itself.

For Pterodactyl, import `distribution/pterodactyl/egg-shardingbase-node.json`, select Java 25, upload `server.jar`, and fill in the same settings. Container memory must cover backend heap, node heap, native/direct memory, and operating overhead.

## Commands, state, and menus

Operator commands are:

- `/shardingbase` — identity, build, feature state, proxy, peer, and help;
- `/shardingbase reload` — atomically reload Shardingbase identity/menu data and revalidate, without reloading Paper or plugins;
- `/shardingbase sync` — open the player/world management GUI.

Permissions `shardingbase.admin`, `shardingbase.reload`, and `shardingbase.sync` default to operators. Menu layouts are generated under `config/shardingbase_menus/` and validate titles, rows, materials, slots, lore, and enabled buttons.

Feature states are:

- `PENDING` while validation is running;
- `ENABLED` after both authenticated backends have matching protocol, Minecraft, and Shardingbase versions;
- `DISABLED` when distributed features are unavailable but ordinary Paper can safely continue;
- `MAINTENANCE` when a sharded world cannot safely accept players or lose ownership enforcement.

The player-data menu controls portable categories including inventory, equipment, ender chest, experience, health, hunger, effects, game mode/abilities, advancements, and statistics. Managed transfers use monotonically increasing SQLite revisions and ignore stale/duplicate state. The confirmed bulk action queues a one-way snapshot of every currently online player; offline player-file migration is not implemented in this prototype.

## World planning and offline cut

The world planner scans Anvil location tables without generating terrain, renders generated chunks, stores tiles in SQLite, and returns a one-use operator link. The browser selects X or Z, a chunk-aligned cut, and which backend owns each half.

An immutable signed transaction then coordinates:

1. matching backend/node/version and capacity preflight;
2. maintenance mode, player removal, save/flush, and dual authorization;
3. graceful child shutdown while the node remains online;
4. mandatory complete backups and durable phase journals;
5. terrain, entity, and POI region splitting, including negative regions and partial boundary files;
6. resumable one-MiB relay with SHA-256 manifests and backpressure;
7. target verification/atomic installation before source commitment;
8. matching shard manifests, target-first restart, health checks, source restart, and finalization;
9. rollback from retained backups on a mutating-phase failure.

A committed world contains `shardingbase-shard.properties`. Runtime ownership clips ordinary chunk loads/tickets/generation to the local half, exposes `LOCAL`/`REMOTE` through the API, displays boundary warnings, and maintenance-locks a sharded backend when coordination is lost.

This remains prototype code. Do not perform the first cut against the only copy of a production world, and do not delete retained transaction backups until both shards have been independently verified.

## Shardingbase API and remote commands

Plugins can keep using Paper APIs locally. Explicit remote block/entity work is asynchronous:

```java
ShardingbaseService service = Shardingbase.service();
WorldPosition position = new WorldPosition("minecraft:overworld", 25_000, 64, 0);

service.remoteOperations().readBlock(position).thenAccept(result -> {
    // Success, Timeout, Unavailable, ValidationFailure, or RemoteFailure
});
```

Remote operations are executed on the target server thread, correlated, bounded, deduplicated, and returned as typed results. Ordinary attempts to force-load an unowned chunk fail fast with a rate-limited diagnostic.

Velocity can expose a command root owned only by the peer when that root is explicitly listed in `remote-command-allowlist`. The local command always wins on conflicts. Remote execution uses a captured console-compatible sender and does not pretend a remote player object exists.

## Verification

Module and Paper compatibility gates:

```powershell
.\gradlew.bat :shardingbase-common:check :shardingbase-node:check :shardingbase-velocity:check
.\gradlew.bat :paper-api:checkstyleMain :paper-server:compileJava
```

Real plugin profiles (downloads are pinned and SHA-512 verified):

```powershell
.\tests\run-plugin-matrix.ps1 -Profile core,worldedit,fawe
```

Real official Velocity plus two packaged nodes/backends:

```powershell
.\tests\run-network-smoke.ps1
```

The integration harness verifies controller TLS startup, shaded SQLite initialization, two persistent authenticated node sessions, both backend validations, and clean coordinated shutdown. It does not simulate a real Minecraft client, so live movement/handoff still requires an in-game acceptance test before production use.

Paper source changes continue to use the paperweight patch workflow. Shardingbase retains Paper's licensing and contributor history; see `CONTRIBUTING.md`, `LICENSE.md`, and the [Paper documentation](https://docs.papermc.io/).
