import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.10"
    kotlin("plugin.serialization") version "1.7.10"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()  
    maven("https://packages.confluent.io/maven/")
}
dependencies {
    implementation("com.github.thake.avro4k:avro4k-kafka-serializer:0.14.0")
    implementation("com.github.avro-kotlin.avro4k:avro4k-core:1.7.0")
    implementation("org.testcontainers:testcontainers:1.17.3")
    implementation("org.testcontainers:kafka:1.17.3")
    implementation("org.apache.kafka:kafka-streams:3.2.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.4.0")
    runtimeOnly("ch.qos.logback:logback-classic:1.3.0-beta0")
    runtimeOnly("ch.qos.logback:logback-core:1.3.0-beta0")
    //implementation(libs.bundles.logging)
    //testImplementation(kotlin("test"))
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}