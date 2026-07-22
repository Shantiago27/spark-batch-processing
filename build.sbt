ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "examen"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.4",
  "org.apache.spark" %% "spark-core" % "3.2.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)