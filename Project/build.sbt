name := "Project"

version := "0.1"

scalaVersion := "2.11.12"

organization := "nl.rug.sc" // Organization name, used when packaging

val sparkVersion = "2.2.1" // Latest version

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core"      % sparkVersion, // Basic Spark library
  "org.apache.spark" %% "spark-mllib"     % sparkVersion, // Machine learning library
  "org.apache.spark" %% "spark-streaming" % sparkVersion, // Streaming library
  "org.apache.spark" %% "spark-sql"       % sparkVersion, // SQL library
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion, //mongo connector
  "org.apache.spark" %% "spark-graphx"    % sparkVersion,  // Graph library
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion, //Kafka connector
  "com.typesafe" % "config" % "1.3.2"
)

