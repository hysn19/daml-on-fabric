import Dependencies._

ThisBuild / scalaVersion := "2.12.11"
ThisBuild / version := "2.0.0"
ThisBuild / organization := "com.daml"
ThisBuild / organizationName := "Digital Asset. LLC"

//TODO BH: run old fabric main until simplified api is fully wired
ThisBuild / mainClass := Some("com.daml.DamlOnFabricServer")

lazy val sdkVersion = "1.3.0"
lazy val akkaVersion = "2.6.1"
lazy val logbackVersion = "1.2.3"
lazy val jacksonDataFormatYamlVersion = "2.11.0"
lazy val protobufVersion = "3.7.1"
lazy val fabricSdkVersion = "2.1.0"

// This task is used by the integration test to detect which version of Ledger API Test Tool to use.
val printSdkVersion = taskKey[Unit]("printSdkVersion")
printSdkVersion := println(sdkVersion)

assemblyMergeStrategy in assembly := {
  // Looks like multiple versions patch versions of of io.netty are getting
  // into dependency graph, choose one.
  case "META-INF/io.netty.versions.properties" =>
    MergeStrategy.first
  // clashing bouncycastle metainfo
  case "META-INF/versions/9/module-info.class" => MergeStrategy.first
  // Both in protobuf and akka
  case PathList("google", "protobuf", n) if n.endsWith(".proto") =>
    MergeStrategy.first
  // In all 2.10 Jackson JARs
  case "module-info.class" =>
    MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in (Compile, run) := Some("com.daml.DamlOnFabricServer")
assemblyJarName in assembly := "daml-on-fabric.jar"

lazy val root = (project in file("."))
  .settings(
    name := "DAML-on-Fabric",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      // DAML
      "com.daml" % "daml-lf-dev-archive-java-proto" % sdkVersion,
      "com.daml" %% "contextualized-logging" % sdkVersion,
      "com.daml" %% "daml-lf-archive-reader" % sdkVersion,
      "com.daml" %% "daml-lf-data" % sdkVersion,
      "com.daml" %% "daml-lf-engine" % sdkVersion,
      "com.daml" %% "daml-lf-language" % sdkVersion,
      "com.daml" %% "daml-lf-transaction" % sdkVersion,
      "com.daml" %% "sandbox" % sdkVersion,
      "com.daml" %% "ledger-api-auth" % sdkVersion,
      // Caching
      "com.daml" %% "caching" % sdkVersion,
      "com.github.blemale" %% "scaffeine" % "3.1.0",
      // DAML kvutils
      "com.daml" %% "participant-state" % sdkVersion,
      "com.daml" %% "participant-state-kvutils" % sdkVersion,
      "com.daml" %% "participant-state-kvutils-app" % sdkVersion,
      "com.daml" %% "testing-utils" % sdkVersion % Test,
      "com.daml" %% "daml-lf-transaction-test-lib" % sdkVersion % Test,
      // Akka
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      // Protobuf / grpc
      "com.google.protobuf" % "protobuf-java-util" % protobufVersion, //  in current setup: need to ALWAYS use the same version as fabric-sdk-java

      // Logging and monitoring
      "org.slf4j" % "slf4j-api" % "1.7.26",
      "ch.qos.logback" % "logback-core" % logbackVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      // fabric
      ("org.hyperledger.fabric-sdk-java" % "fabric-sdk-java" % fabricSdkVersion)
        .excludeAll(ExclusionRule(organization = "javax.xml.bind", name = "jaxb-api")),
      "org.jodd" % "jodd-json" % "5.0.12",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonDataFormatYamlVersion,

      // eosio-rpc-wrapper
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.11.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.11.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.2",
      "com.squareup.retrofit2" % "converter-jackson" % "2.9.0",
      "com.squareup.retrofit2" % "converter-scalars" % "2.9.0",
      "com.squareup.retrofit2" % "retrofit" % "2.9.0",
      "com.squareup.okhttp3" % "logging-interceptor" % "4.8.0",
      "com.squareup.okhttp3" % "okhttp" % "4.8.0",
      "org.apache.commons" % "commons-lang3" % "3.11",

      // com.googlecode.json-simple/json-simple
      "com.googlecode.json-simple" % "json-simple" % "1.1"

    ),
    resolvers += Resolver.mavenLocal,
    useCoursier := false
  )
