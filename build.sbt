name := "cartesianproduct2"

version := "1.0"

scalaVersion := "2.11.0"

val sparkVersion = "2.1.0"

lazy val akkaVersion = "2.5.3"

libraryDependencies ++= Seq(

//"org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "0.10.1.1",
"com.typesafe.akka" %% "akka-actor" % akkaVersion,
"com.typesafe.akka" %% "akka-testkit" % akkaVersion,
"org.scalatest" %% "scalatest" % "3.0.1" % "test"

)


        