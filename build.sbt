name := "cartesianproduct2"
version := "1.0"
scalaVersion := "2.11.0"
val sparkVersion = "2.0.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "0.10.1.1",
  "net.minidev" % "json-smart" % "2.1.1",
  "junit" % "junit" % "4.10" % "test"
)
