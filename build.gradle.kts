import io.papermc.paperweight.checkstyle.PaperCheckstyleExt
import io.papermc.paperweight.checkstyle.tasks.PaperCheckstyleTask
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    id("io.papermc.paperweight.core") version "2.0.0-beta.21" apply false
}

subprojects {
    apply {
        plugin("java-library")
        plugin("maven-publish")
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(25)
        }
    }

    val tempDisabled = setOf("paper-server", "paper-generator", "test-plugin")

    if (name !in tempDisabled) {
        apply { plugin("io.papermc.paperweight.paper-checkstyle") }
        extensions.configure<PaperCheckstyleExt> {
            typeUseAnnotationsFile.set(rootProject.layout.projectDirectory.file(".checkstyle/type_use_annotations.txt"))
        }

        /*tasks.withType<PaperCheckstyleTask>().configureEach {
            configDirectory = rootProject.layout.projectDirectory.dir(".checkstyle")
            // configFile = layout.projectDirectory.file(".checkstyle/checkstyle.xml").asFile // use the base file if not overwritten
            maxHeapSize = "2g"
            reports {
                xml.required = true
                html.required = true
            }
        }*/

        dependencies {
            "checkstyle"(project(":paper-checkstyle"))
        }
    }
}

val paperMavenPublicUrl = "https://repo.papermc.io/repository/maven-public/"

subprojects {
    tasks.withType<JavaCompile>().configureEach {
        options.encoding = Charsets.UTF_8.name()
        options.release = 25
        options.isFork = true
        options.compilerArgs.addAll(listOf("-Xlint:-deprecation", "-Xlint:-removal"))
    }
    tasks.withType<Javadoc>().configureEach {
        options.encoding = Charsets.UTF_8.name()
    }
    tasks.withType<ProcessResources>().configureEach {
        filteringCharset = Charsets.UTF_8.name()
    }
    tasks.withType<Test>().configureEach {
        testLogging {
            showStackTraces = true
            exceptionFormat = TestExceptionFormat.FULL
            events(TestLogEvent.STANDARD_OUT)
        }
    }

    repositories {
        mavenCentral()
        maven(paperMavenPublicUrl)
    }

    extensions.configure<PublishingExtension> {
        repositories {
            maven("https://artifactory.papermc.io/artifactory/releases/") {
                name = "paperReleases"
                credentials(PasswordCredentials::class)
            }
        }
    }
}

tasks.register("printMinecraftVersion") {
    val mcVersion = providers.gradleProperty("mcVersion")
    doLast {
        println(mcVersion.get().trim())
    }
}

tasks.register("printPaperVersion") {
    val paperVersion = provider { project.version }
    doLast {
        println(paperVersion.get())
    }
}

val installShardingbaseBackend by tasks.registering {
    group = "build"
    description = "Install the standalone Shardingbase backend in the project root"
    dependsOn(":paper-server:createShardingbaseJar")
    val source = layout.projectDirectory.file("paper-server/build/libs/Shardingbase.jar")
    val target = layout.projectDirectory.file("Shardingbase.jar")
    inputs.file(source)
    outputs.file(target)
    doLast {
        java.nio.file.Files.copy(
            source.asFile.toPath(),
            target.asFile.toPath(),
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        )
    }
}

val assembleShardingbaseRelease by tasks.registering(Sync::class) {
    group = "build"
    description = "Assemble the Shardingbase server, Velocity, and node test artifacts"
    dependsOn(
        ":paper-server:createShardingbaseJar",
        ":shardingbase-velocity:jar",
        ":shardingbase-node:jar",
        installShardingbaseBackend,
    )
    into(layout.buildDirectory.dir("release"))
    from(layout.projectDirectory.file("paper-server/build/libs/Shardingbase.jar"))
    from(layout.projectDirectory.file("shardingbase-velocity/build/libs/Shardingbase-Velocity.jar"))
    from(layout.projectDirectory.file("shardingbase-node/build/libs/Shardingbase-Node.jar"))
}

tasks.register("buildShardingbaseCompatibilityFixtures") {
    group = "verification"
    description = "Build representative Bukkit, legacy Spigot, and Paper compatibility plugins"
    dependsOn(
        ":shardingbase-fixture-bukkit:check",
        ":shardingbase-fixture-bukkit:jar",
        ":shardingbase-fixture-spigot:check",
        ":shardingbase-fixture-spigot:jar",
        ":shardingbase-fixture-paper:check",
        ":shardingbase-fixture-paper:jar",
    )
}
