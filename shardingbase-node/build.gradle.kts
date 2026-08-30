plugins {
    application
}

group = "dev.shardingbase"

dependencies {
    implementation(project(":shardingbase-common"))
}

application {
    mainClass = "dev.shardingbase.node.ShardingbaseNode"
}

tasks.jar {
    dependsOn(project(":shardingbase-common").tasks.jar)
    archiveBaseName = "Shardingbase-Node"
    manifest.attributes["Main-Class"] = application.mainClass.get()
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from({
        configurations.runtimeClasspath.get().map { dependency ->
            if (dependency.isDirectory) dependency else zipTree(dependency)
        }
    })
}
