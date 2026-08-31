plugins {
    java
}

group = "dev.shardingbase"

val velocityApiVersion = "4.1.0-SNAPSHOT"

dependencies {
    implementation(project(":shardingbase-common"))
    implementation("org.yaml:snakeyaml:2.2")
    implementation("org.xerial:sqlite-jdbc:3.49.1.0")
    compileOnly("com.velocitypowered:velocity-api:$velocityApiVersion")
    annotationProcessor("com.velocitypowered:velocity-api:$velocityApiVersion")
    testImplementation("com.velocitypowered:velocity-api:$velocityApiVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:6.0.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.3")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    dependsOn(project(":shardingbase-common").tasks.jar)
    archiveFileName = "shardingbase-velocity.jar"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from({
        configurations.runtimeClasspath.get().map { dependency ->
            if (dependency.isDirectory) dependency else zipTree(dependency)
        }
    })
}
