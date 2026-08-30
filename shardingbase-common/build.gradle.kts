plugins {
    `java-library`
    `maven-publish`
}

group = "dev.shardingbase"

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api("org.jspecify:jspecify:1.0.0")
    testImplementation("org.junit.jupiter:junit-jupiter:6.0.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.0.3")
}

tasks.test {
    useJUnitPlatform()
}
