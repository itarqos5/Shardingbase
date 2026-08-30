plugins {
    java
}

group = "dev.shardingbase"

val velocityApiVersion = "4.1.0-SNAPSHOT"

dependencies {
    implementation(project(":shardingbase-common"))
    compileOnly("com.velocitypowered:velocity-api:$velocityApiVersion")
    annotationProcessor("com.velocitypowered:velocity-api:$velocityApiVersion")
}

tasks.jar {
    dependsOn(project(":shardingbase-common").tasks.jar)
    archiveFileName = "Shardingbase-Velocity.jar"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from({
        configurations.runtimeClasspath.get().map { dependency ->
            if (dependency.isDirectory) dependency else zipTree(dependency)
        }
    })
}
