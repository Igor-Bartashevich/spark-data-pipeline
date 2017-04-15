name := "spark-data-pipeline"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.json4s" %% "json4s-native" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.0.1"
)
    