plugins {
    application
}

group = "dev.shardingbase"

dependencies {
    implementation(project(":shardingbase-common"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.0.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.3")
}

application {
    mainClass = "dev.shardingbase.node.ShardingbaseNode"
}

tasks.jar {
    dependsOn(project(":shardingbase-common").tasks.jar)
    dependsOn(":paper-server:createShardingbaseJar")
    archiveFileName = "server.jar"
    manifest.attributes["Main-Class"] = application.mainClass.get()
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(rootProject.layout.projectDirectory.file("paper-server/build/libs/Shardingbase.jar")) {
        into("META-INF/shardingbase")
        rename { "backend.jar" }
    }
    from({
        configurations.runtimeClasspath.get().map { dependency ->
            if (dependency.isDirectory) dependency else zipTree(dependency)
        }
    })
}

tasks.test {
    useJUnitPlatform()
}
