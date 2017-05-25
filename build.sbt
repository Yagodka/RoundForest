
lazy val root = (project in file(".")).
  settings(
    name := "round_forest",
    version := "1.0",
    scalaVersion := "2.11.11",
    organization := "com.roundforest",
    libraryDependencies ++= Seq(
      sparkDeps,
      testDeps
    ).flatten,
    scalacOptions := Seq(
      "-unchecked",
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:_")
  )

lazy val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.2"
)

lazy val testDeps = Seq(
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)