name := "cartesianproduct2"

version := "1.0"

scalaVersion := "2.11.0"

val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion,
  "org.apache.kafka" % "kafka_2.11" % "0.10.2.1",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
  "net.minidev" % "json-smart" % "2.1.1"
)