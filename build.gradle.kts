import com.github.davidmc24.gradle.plugin.avro.GenerateAvroJavaTask

plugins {
    id("java")
    id("com.github.davidmc24.gradle.plugin.avro") version ("1.9.1")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenLocal()
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    implementation("org.apache.avro:avro:1.12.0")
    // Note: this artifact is located at Confluent repository (https://packages.confluent.io/maven/)
    implementation("io.confluent:kafka-avro-serializer:7.7.2")

    implementation("org.slf4j:slf4j-simple:2.1.0-alpha1")

    implementation("org.apache.kafka:kafka-streams:3.9.0")
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register("generateAvro", GenerateAvroJavaTask::class) {
    // Avro schema files
    source("src/main/resources/avro")
    // generated classes
    setOutputDir(File("src/main/java"))
}