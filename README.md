# Shardingbase

[![Build status](https://img.shields.io/github/actions/workflow/status/itarqos5/Shardingbase/build.yml?branch=feature%2Fshardingbase-prototype&label=build)](https://github.com/itarqos5/Shardingbase/actions)
[![Upstream](https://img.shields.io/badge/upstream-Paper-344ceb)](https://github.com/PaperMC/Paper)

Shardingbase is an experimental Paper 26.2 fork for one Velocity proxy and
exactly two backend servers. It keeps the Bukkit, Spigot, and Paper plugin
loaders and public API packages intact for ordinary local-server behavior, then
adds explicit shard identity, authenticated coordination, and an asynchronous
`dev.shardingbase.api` contract.

This branch is a working prototype, not production-ready distributed storage.
Never test a world cut against the only copy of a world. Features described as
prototype-only below are deliberately reported as unavailable instead of
pretending that remote Bukkit objects exist.

## Artifacts

Build with JDK 25:

```bash
./gradlew assembleShardingbaseRelease
```

Windows PowerShell:

```powershell
.\gradlew.bat assembleShardingbaseRelease
```

The task writes these files to `build/release/`:

- `Shardingbase.jar` — the standalone Paper-compatible backend.
- `Shardingbase-Velocity.jar` — the Velocity 4.1 controller plugin.
- `Shardingbase-Node.jar` — the backend parent process, containing an embedded
  copy of the backend.

The root `Shardingbase.jar` is also the standalone backend. The node remains a
separate, clearly named artifact.

## Installation

### Bare metal

For a normal backend without node supervision:

```bash
java -Xms128M -Xmx4G -jar Shardingbase.jar nogui
```

Shardingbase still runs as a Paper-compatible server when the controller is
absent, but distributed features enter `DISABLED`.

For a managed backend, place only `Shardingbase-Node.jar` in an otherwise empty
backend directory and run:

```bash
java -Xms128M -Xmx4G -jar Shardingbase-Node.jar nogui
```

The node exports its embedded server as `Shardingbase-backend.jar`, starts it in
a child JVM, shares the console, forwards Minecraft arguments, and propagates a
non-zero child exit. Set `SHARDINGBASE_BACKEND_JAR` to another plain `.jar`
filename if required. Set `SHARDINGBASE_BACKEND_MEMORY_MB` when the node itself
has a small bounded heap; the child then receives `-Xms128M` and the configured
`-Xmx` instead of inheriting the node heap.

Install `Shardingbase-Velocity.jar` in `velocity/plugins/`, start Velocity once,
and inspect `plugins/shardingbase/config.yml`. The plugin generates exactly two
node credentials, a PKCS12 TLS key, a certificate fingerprint in the proxy log,
and `shardingbase.db`.

### Pterodactyl

Import `distribution/pterodactyl/egg-shardingbase-node.json`, select its Java 25
image, then upload `Shardingbase-Node.jar`. The default jar filename is already
correct. Configure:

- `NODE_MEMORY` for the parent (128 MiB by default).
- `SHARDINGBASE_BACKEND_MEMORY_MB` for the Minecraft child.
- `SHARDINGBASE_CONTROLLER_URI`, such as `tls://proxy.example.net:8443`.
- `SHARDINGBASE_CERTIFICATE_SHA256`, copied from the Velocity startup log.
- `SHARDINGBASE_NODE_ID` and its matching `SHARDINGBASE_NODE_CREDENTIAL`.
- the backup root and retention policy before enabling world transactions.

The panel allocation must cover backend heap, node heap, direct/native memory,
and operating overhead. The Velocity control listener needs a separate reachable
allocation in addition to the normal Minecraft proxy port.

## Backend configuration and states

Before plugins load, each backend creates `config/shardingbase.yml` containing
exactly:

```yaml
server-id: "generated-unique-id"
server-name: "velocity-server-name"
```

`server-name` must exactly match a Velocity `servers` entry. Each backend needs
a unique non-empty `server-id`. A new file uses a generated UUID and
`change-me`; edit that name and run `/shardingbase reload` or restart.

Missing IDs and scalar type mistakes are repaired with a timestamped backup and
atomic replacement. Duplicate keys, unreadable YAML, structured values, or an
unsafe write fail startup clearly. A valid but unknown server name leaves normal
Paper operation online with Shardingbase `DISABLED`.

Feature states are:

- `PENDING` while validation is outstanding.
- `ENABLED` only after both authenticated backends have matching protocol,
  Minecraft, and Shardingbase versions.
- `DISABLED` when validation fails or times out; retries use capped backoff.
- `MAINTENANCE` for a registered shard that cannot safely accept players.

Operator commands and permissions (all operator-only by default):

- `/shardingbase` — identity, version, feature state, proxy, peer, and help.
- `/shardingbase reload` — reload only identity/menu configuration and revalidate.
- `/shardingbase sync` — open the player-only management GUI.
- `shardingbase.admin`, `shardingbase.reload`, and `shardingbase.sync`.

## Menus

Validated menu files are created under `config/shardingbase_menus/`:

- `main.yml`
- `player-data.yml`
- `confirmation.yml`
- `world-sharding.yml`

Titles, row counts, materials, slots, display names, lore, and enabled buttons
are configurable. Invalid menu data is backed up and repaired or replaced by an
in-memory built-in default without changing `shardingbase.yml`. Menu holders are
typed, clicks and drags are cancelled after plugin event dispatch, and every
operation re-checks permission and feature state.

## Security and controller configuration

The node-to-proxy control plane uses TLS 1.3 with mandatory SHA-256 certificate
pinning and per-node credentials. Frames are length-prefixed, versioned,
checksummed, size-bounded, correlated, and replay-checked. The local
backend-to-node hop is loopback-only and uses a random per-launch token.

Velocity's generated configuration resembles:

```yaml
control:
  bind: 0.0.0.0
  port: 8443
  keystore: tls.p12
  keystore-password: generated-secret
database: shardingbase.db
node-credentials:
  node-a: generated-secret
  node-b: generated-secret
```

Keep the keystore password and node credentials secret. Expose only the control
port needed by the two nodes, and use a private network or firewall allowlist in
addition to TLS. Re-enroll a node whenever the certificate changes by updating
its pinned fingerprint.

## Plugin compatibility and API

Local Bukkit, Spigot, and Paper plugins retain their normal loaders, descriptors,
events, and API packages. A plugin running on one shard does not receive fake
remote `World`, `Block`, `Entity`, or `Player` instances. Cross-shard work must
use the asynchronous API:

```java
ShardingbaseService service = Shardingbase.service();
Ownership owner = service.ownership(new WorldPosition("minecraft:overworld", 0, 64, 0));
service.remoteOperations().readBlock(position).thenAccept(result -> {
    // Handle Success, Timeout, Unavailable, ValidationFailure, or RemoteFailure.
});
```

The current prototype publishes the type-safe API and identity/peer status.
Remote operation execution is not enabled yet and returns a typed unavailable
result. Ordinary local plugin behavior remains unchanged.

## Prototype implementation status

Implemented and tested in this branch:

- strict backend identity lifecycle, feature states, reload, permissions, and
  `/shardingbase` commands;
- four configurable GUI schemas and protected inventory interaction;
- framed protocol validation, corruption/limit checks, replay window, pinned TLS,
  per-node authentication, and SQLite uniqueness/version validation;
- a self-extracting child-JVM node with distinct node/backend memory limits;
- raw Anvil region splitting by X or Z chunk boundary for terrain, entities, and
  POI data, including negative region coordinates;
- asynchronous public API result types and peer/identity reporting.

Not yet safe or wired end-to-end:

- portable player snapshot capture/staging and live Velocity handoff;
- remote command catalog/execution and remote operation routing;
- generated-chunk map tiles, HTTPS planner, and WebSocket sessions;
- signed plans, mandatory full backup, relay, journal recovery, rollback, and
  automated two-node restart for world transactions;
- runtime chunk-boundary clipping, particle/border warnings, and automatic
  coordinate-preserving crossing.

The region splitter is a tested primitive, not authorization to mutate a live
world. Do not use it manually against production data.

## Development and verification

Focused checks:

```bash
./gradlew :shardingbase-common:check :shardingbase-node:check :shardingbase-velocity:check
./gradlew :paper-api:checkstyleMain :paper-server:compileJava
```

Paper source changes continue to use the patch workflow:

```bash
./gradlew applyPatches
./gradlew fixupSourcePatches
./gradlew rebuildPatches
```

Shardingbase retains Paper's licensing and contributor history. Verify defects on
an unmodified Paper build before reporting them upstream; Shardingbase-specific
issues belong in this repository. See `CONTRIBUTING.md`, `LICENSE.md`, and the
[Paper documentation](https://docs.papermc.io/) for the compatible base platform.
