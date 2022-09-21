ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-in-action",
    idePackagePrefix := Some("com.andy"),
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % "3.2.2",
      "com.typesafe.play" %% "play-json" % "2.9.3",
      "co.fs2" %% "fs2-core" % "3.3.0",
      "com.lihaoyi" %% "os-lib" % "0.8.1",
      "org.scalatest" %% "scalatest" % "3.2.13" % "test"
    )
  )
