plugins {
    java
    application
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
    implementation("mysql:mysql-connector-java:8.0.29")
}

application {
    mainClass.set("lab.App")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
