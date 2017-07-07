name := "cartesianproduct2"

version := "1.0"

scalaVersion := "2.11.0"

val sparkVersion = "2.1.0"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies ++= Seq(

//  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % "0.10.1.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"
)


        
