# Shardingbase

[![Build status](https://img.shields.io/github/actions/workflow/status/itarqos5/Shardingbase/build.yml?branch=main&label=build)](https://github.com/itarqos5/Shardingbase/actions)
[![Upstream](https://img.shields.io/badge/upstream-Paper-344ceb)](https://github.com/PaperMC/Paper)

Shardingbase is an experimental [Paper](https://papermc.io/) fork that keeps
Paper API compatibility while providing a foundation for shard-aware Minecraft
server networks.

The project is in early development. The current prototype establishes a
distinct server identity, a versioned shared protocol, a backend node process,
and a Velocity controller plugin. It is not yet a production-ready sharding
implementation.

## Components

- `paper-api` and `paper-server` — the Paper-compatible API and Shardingbase
  server runtime.
- `shardingbase-common` — protocol identifiers shared by Shardingbase
  processes.
- `shardingbase-node` — the backend supervisor/transaction-agent entry point.
- `shardingbase-velocity` — the Velocity-side network controller.

## Building

Building requires JDK 25 and an internet connection.

On Windows:

```powershell
.\gradlew.bat :paper-server:createShardingbaseJar
```

On Linux or macOS:

```bash
./gradlew :paper-server:createShardingbaseJar
```

The runnable server is written to
`paper-server/build/libs/Shardingbase.jar`. The node and Velocity artifacts can
be assembled with:

```bash
./gradlew :shardingbase-node:build :shardingbase-velocity:build
```

To create the complete test layout, run:

```bash
./gradlew assembleShardingbaseRelease
```

This produces `Shardingbase-server.jar`, `Shardingbase-Velocity.jar`, and
`Shardingbase-Node.jar` in `build/release/`. It also installs the manager as
`Shardingbase.jar` in the project root. The manager contains the server JAR as
an embedded resource. Starting it exports that resource beside the manager as
`Shardingbase-backend.jar`; it does not launch the backend.

Run the focused checks with:

```bash
./gradlew :shardingbase-common:check :shardingbase-node:check :shardingbase-velocity:check
```

## Paper compatibility

Shardingbase deliberately retains Paper's API coordinates and reports Paper as
a compatible brand so existing Paper plugins can continue to recognize the
server. Paper documentation and API references remain applicable unless a
Shardingbase-specific document says otherwise:

- [Paper documentation](https://docs.papermc.io/)
- [Paper API Javadocs](https://jd.papermc.io/paper/)
- [Paper upstream repository](https://github.com/PaperMC/Paper)

For Gradle projects that target the compatible Paper API:

```kotlin
repositories {
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:26.2.build.+")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
}
```

## Upstream attribution

Shardingbase is based on Paper and retains Paper's licensing and contributor
history. Bugs that also occur on an unmodified Paper build should be verified
against Paper before being reported upstream. Shardingbase-specific behavior
belongs in this repository.

See [CONTRIBUTING.md](CONTRIBUTING.md) and [LICENSE.md](LICENSE.md) for the
upstream contribution terms and licenses included in this fork.
