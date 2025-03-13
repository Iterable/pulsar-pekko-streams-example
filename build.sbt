ThisBuild / scalaVersion := "3.3.3"

lazy val root = (project in file("."))
  .settings(
    name := "iterable-pulsar-example",
    organization := "com.iterable",
    version := "0.1.0",
    scalaVersion := "3.3.3",

    // Java settings remain the same
    javacOptions ++= Seq("-source", "21", "-target", "21"),
    javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx2G",
      "-XX:+UseG1GC"
    ),
    resolvers += "Apache Repository" at "https://repository.apache.org/content/repositories/releases",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-Xfatal-warnings",
      "-unchecked",
      "-explain"
    ),
    scalafmtOnCompile := true,

    // Dependencies that don't need cross-compilation (pure Java)
    libraryDependencies ++= Seq(
      "org.apache.pulsar" % "pulsar-client" % "3.3.4",
      "org.apache.pulsar" % "pulsar-client-admin" % "3.3.4",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      "com.typesafe.play" %% "play-json" % "2.10.4",
      "org.apache.pekko" %% "pekko-stream" % "1.0.2",
      "dev.zio" %% "zio-streams" % "2.0.21",
      "dev.zio" %% "zio-interop-reactivestreams" % "2.0.0"
    ),
    Test / fork := true,
    Test / parallelExecution := false
  )

// Debug task to confirm resource paths
val printResourceDirectories = taskKey[Unit]("Print all resource directories")
printResourceDirectories := {
  val resources = (Compile / resourceDirectory).value

  println(s"Main resourceDirectory: ${resources}")
}
