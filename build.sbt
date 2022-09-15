ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-in-action",
    idePackagePrefix := Some("com.andy"),
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % "3.2.2",
    )
  )
