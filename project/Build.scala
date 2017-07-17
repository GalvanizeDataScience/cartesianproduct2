/**
  * Created by michaelseeber on 7/11/17.
  */

import sbt._
import sbtassembly._
import sbtassembly.AssemblyKeys._
import Keys._

object CartesianBuild extends Build {
  lazy val sparkVersion = "2.0.2"
  lazy val commonDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
    "org.apache.kafka" %% "kafka" % "0.10.1.1",
    "org.apache.kafka" % "kafka-clients" % "0.10.1.1",
    "net.minidev" % "json-smart" % "2.1.1",
    "junit" % "junit" % "4.10" % "test",
    "com.typesafe.akka" %% "akka-actor" % "2.5.3"
  )


  lazy val commonSettings = Seq(
    organization := "cartesianproduct2",
    version := "0.1.0",
    scalaVersion := "2.11.0",
    libraryDependencies ++= commonDependencies,
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )


  lazy val helpers = Project(id ="cartesian-helpers", base = file("helpers"))
    .settings(commonSettings)

  lazy val root = Project(id = "cartesian-root", base = file("."))
    .settings(commonSettings) dependsOn(helpers)

  lazy val kafka_cassandra = Project(id ="cartesian-kafka_cassandra", base = file("kafka_cassandra"))
    .settings(commonSettings) dependsOn(helpers)

  lazy val geo_table = Project(id ="cartesian-geo_table", base = file("geo_table"))
    .settings(commonSettings) dependsOn(helpers)

  lazy val kafka_spark = Project(id ="cartesian-kafka_spark", base = file("kafka_spark"))
    .settings(commonSettings) dependsOn(helpers)

  lazy val weather_kafka_cassandra = Project(id ="cartesian-weather_kafka_cassandra", base = file("weather_kafka_cassandra"))
    .settings(commonSettings) dependsOn(helpers)

}





