import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val ktor_version: String by project
val kotlin_version: String by project
val logback_version: String by project

plugins {
    base
    kotlin("jvm") version "1.3.60"
}

group = "de.letescape"
version = "0.0.1"

repositories {
    mavenLocal()
    jcenter()
    maven { url = uri("https://kotlin.bintray.com/ktor") }
    maven { url = uri("https://kotlin.bintray.com/kotlinx") }
}

dependencies {
    compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8:$kotlin_version")
    compile("ch.qos.logback:logback-classic:$logback_version")
    compile("io.ktor:ktor-client-core:$ktor_version")
    compile("io.ktor:ktor-client-core-jvm:$ktor_version")
    compile("io.ktor:ktor-client-apache:$ktor_version") // remove once we move samples out of library
    compile("io.ktor:ktor-client-json-jvm:$ktor_version")
    compile("io.ktor:ktor-client-gson:$ktor_version")
    testCompile("io.ktor:ktor-server-tests:$ktor_version")
}

kotlin.sourceSets["main"].kotlin.srcDirs("src")
kotlin.sourceSets["test"].kotlin.srcDirs("test")

sourceSets["main"].resources.srcDirs("resources")
sourceSets["test"].resources.srcDirs("testresources")
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    freeCompilerArgs = listOf("-Xinline-classes")
}