ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

// Spark's internal reflection touches JDK-internal packages (e.g. sun.nio.ch)
// that the module system hides by default from Java 9+. spark-submit's launch
// scripts add these flags for you; running Spark embedded from sbt does not,
// so without them SparkContext initialization fails on JDK 17 with
// java.lang.IllegalAccessError: ... cannot access class sun.nio.ch.DirectBuffer.
// Forked JVMs otherwise inherit the OS default charset; pin UTF-8 explicitly
// so accented source literals (á, é, ñ, ...) print correctly on every machine,
// regardless of its default codepage.
val utf8ConsoleOpts = Seq(
  "-Dfile.encoding=UTF-8",
  "-Dsun.jnu.encoding=UTF-8"
)

val sparkJava17Opens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-batch-processing",
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-feature"),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= sparkJava17Opens ++ utf8ConsoleOpts,
    Test / fork := true,
    Test / javaOptions ++= sparkJava17Opens ++ utf8ConsoleOpts
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.8",
  "org.apache.spark" %% "spark-core" % "3.5.8",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)
