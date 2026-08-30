plugins {
    java
}

group = "dev.shardingbase.fixtures"

dependencies {
    compileOnly(project(":paper-api"))
}

tasks.jar {
    archiveFileName = "Shardingbase-Fixture-Paper.jar"
}
