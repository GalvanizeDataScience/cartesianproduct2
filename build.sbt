
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
  "net.minidev" % "json-smart" % "2.1.1",
  "junit" % "junit" % "4.10" % "test",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
)

// See documentation in ProgFunBuild.scala
assignmentsMap := {
  val styleSheetPath = (baseDirectory.value / ".." / ".." / "project" / "scalastyle_config.xml").getPath
  Map(
    "example" -> Assignment(
      packageName = "example",
      key = "g4unnjZBEeWj7SIAC5PFxA",
      itemId = "xIz9O",
      partId = "d5jxI",
      maxScore = 10d,
      styleScoreRatio = 0.2,
      styleSheet = styleSheetPath),
    "recfun" -> Assignment(
      packageName = "recfun",
      key = "SNYuDzZEEeWNVyIAC92BaQ",
      itemId = "Ey6Jf",
      partId = "PzVVY",
      maxScore = 10d,
      styleScoreRatio = 0.2,
      styleSheet = styleSheetPath),
    "funsets" -> Assignment(
      packageName = "funsets",
      key = "FNHHMDfsEeWAGiIAC46PTg",
      itemId = "BVa6a",
      partId = "IljBE",
      maxScore = 10d,
      styleScoreRatio = 0.2,
      styleSheet = styleSheetPath),
    "objsets" -> Assignment(
      packageName = "objsets",
      key = "6PTXvD99EeWAiCIAC7Pj9w",
      itemId = "Ogg05",
      partId = "7hlkb",
      maxScore = 10d,
      styleScoreRatio = 0.2,
      styleSheet = styleSheetPath,
      options = Map("grader-timeout" -> "1800")),
    "patmat" -> Assignment(
      packageName = "patmat",
      key = "BwkTtD9_EeWFZSIACtiVgg",
      itemId = "uctOq",
      partId = "2KYZc",
      maxScore = 10d,
      styleScoreRatio = 0.2,
      styleSheet = styleSheetPath),
    "forcomp" -> Assignment(
      packageName = "forcomp",
      key = "CPJe397VEeWLGArWOseZkw",
      itemId = "nVRPb",
      partId = "v2XIe",
      maxScore = 10d,
      styleScoreRatio = 0.2,
      styleSheet = styleSheetPath,
      options = Map("grader-timeout" -> "1800"))
  )
}