pluginManagement {
    repositories {
        gradlePluginPortal()
        maven("https://repo.papermc.io/repository/maven-public/")
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

if (!file(".git").exists()) {
    val errorText = """

        =====================[ ERROR ]=====================
         The Shardingbase project directory is not a properly cloned Git repository.

         In order to build Shardingbase from source you must clone
         the Shardingbase repository using Git, not download a code
         zip from GitHub.

         Clone Shardingbase from
         https://github.com/itarqos5/Shardingbase

         See https://github.com/PaperMC/Paper/blob/main/CONTRIBUTING.md
         for upstream information on building and modifying Paper forks.
        ===================================================
    """.trimIndent()
    error(errorText)
}

rootProject.name = "shardingbase"

for (name in listOf("paper-api", "paper-server")) {
    include(name)
    file(name).mkdirs()
}

include("shardingbase-common")
include("shardingbase-velocity")
include("shardingbase-node")

include("shardingbase-fixture-bukkit")
project(":shardingbase-fixture-bukkit").projectDir = file("compatibility-fixtures/bukkit")
include("shardingbase-fixture-spigot")
project(":shardingbase-fixture-spigot").projectDir = file("compatibility-fixtures/spigot")
include("shardingbase-fixture-paper")
project(":shardingbase-fixture-paper").projectDir = file("compatibility-fixtures/paper")

include("paper-checkstyle")

optionalInclude("test-plugin")
optionalInclude("paper-generator")

fun optionalInclude(name: String, op: (ProjectDescriptor.() -> Unit)? = null) {
    val settingsFile = file("$name.settings.gradle.kts")
    if (settingsFile.exists()) {
        apply(from = settingsFile)
        findProject(":$name")?.let { op?.invoke(it) }
    } else {
        settingsFile.writeText(
            """
            // Uncomment to enable the '$name' project
            // include(":$name")

            """.trimIndent()
        )
    }
}

gradle.lifecycle.beforeProject {
    val mcVersion = providers.gradleProperty("mcVersion").get().trim()
    val paperVersionChannel = providers.gradleProperty("channel").get().trim()
    // Bukkit.getBukkitVersion() is an API compatibility identifier, not the
    // Shardingbase build number. Keep it byte-for-byte compatible with the
    // upstream Paper build this fork tracks so plugins do not mistake a local
    // fork build for an unknown Minecraft/API version.
    val paperCompatibilityBuild = providers.gradleProperty("paperCompatibilityBuild").get().trim().toInt()
    val versionString = "$mcVersion.build.$paperCompatibilityBuild-${paperVersionChannel.lowercase()}"
    version = versionString
}

if (providers.gradleProperty("paperBuildCacheEnabled").orNull.toBoolean()) {
    val buildCacheUsername = providers.gradleProperty("paperBuildCacheUsername").orElse("").get()
    val buildCachePassword = providers.gradleProperty("paperBuildCachePassword").orElse("").get()
    if (buildCacheUsername.isBlank() || buildCachePassword.isBlank()) {
        println("The Paper remote build cache is enabled, but no credentials were provided. Remote build cache will not be used.")
    } else {
        val buildCacheUrl = providers.gradleProperty("paperBuildCacheUrl")
            .orElse("https://gradle-build-cache.papermc.io/")
            .get()
        val buildCachePush = providers.gradleProperty("paperBuildCachePush").orNull?.toBoolean()
            ?: System.getProperty("CI").toBoolean()
        buildCache {
            remote<HttpBuildCache> {
                url = uri(buildCacheUrl)
                isPush = buildCachePush
                credentials {
                    username = buildCacheUsername
                    password = buildCachePassword
                }
            }
        }
    }
}
