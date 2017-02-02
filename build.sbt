name := "storm-tutorial-100"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "1.0.2",
  //"org.apache.storm" % "storm-core" % "1.0.2" % "provided",
  "com.github.pathikrit" %% "better-files" % "2.17.1"
)
