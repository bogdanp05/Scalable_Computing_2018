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
  "com.typesafe" % "config" % "1.3.2",
  "org.scalaj" % "scalaj-http_2.11" % "2.3.0"
)

libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "0.13.2",

  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  "org.scalanlp" %% "breeze-natives" % "0.13.2",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "0.13.2"
) // math lib for matrix factorization


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

