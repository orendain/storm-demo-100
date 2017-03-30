name := "storm-tutorial-100"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "1.0.2" % "provided",
  "com.github.pathikrit" %% "better-files" % "2.17.1",
  "commons-lang" % "commons-lang" % "2.6", // For Storm Metrics on HDP 2.6
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
)
