/**
  * Created by michaelseeber on 7/11/17.
  */

import java.text.DecimalFormat
import scala.util.parsing.json.JSON.parseFull
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.driver.core.Cluster
import org.apache.kafka.common.serialization.StringDeserializer

object SparkStream {
  // Function to parse JSON - from Daniel's "Kafka to Cassandra" code
  class ClassCastingExtractor[T] {
    def unapply(a: Any): Option[T] = {
      Some(a.asInstanceOf[T])
    }
  }

  object MapExtractor extends ClassCastingExtractor[Map[String, Any]]
  object ListExtractor extends ClassCastingExtractor[List[Any]]
  object StringExtractor extends ClassCastingExtractor[String]
  object DoubleExtractor extends ClassCastingExtractor[Double]

  def parseRecord(record: String) = {
    for {
      Some(MapExtractor(map)) <- List(parseFull(record))
      DoubleExtractor(latitude) = map("latitude")
      DoubleExtractor(longitude) = map("longitude")
      ListExtractor(hourly) = map("hourly")
      MapExtractor(most_recent) = hourly.head
      DoubleExtractor(timestamp) = most_recent("timestamp")
      MapExtractor(data) = most_recent("avg")
      MapExtractor(particulate_matter) = data("PM")
      MapExtractor(nitrous_oxide) = data("NO2")
      MapExtractor(particulate_matter_ten) = data("PM10")
      MapExtractor(particulate_matter_twenty_five) = data("PM2_5")
      MapExtractor(ozone) = data("O3")
      MapExtractor(overall) = data("overall")

    } yield {
      (latitude,
        longitude,
        timestamp,
        overall,
        nitrous_oxide,
        ozone,
        particulate_matter,
        particulate_matter_ten,
        particulate_matter_twenty_five
      )
    }
  }

  def main(args: Array[String]): Unit = {

    // Spark and Spark Streaming Contexts
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("spark_conf")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    // Create type and table in Cassandra
    implicit val session = new Cluster
    .Builder()
      .addContactPoints("localhost")
      .withPort(9042)
      .build()
      .connect()

    val create_type = s"CREATE TYPE IF NOT EXISTS plume.poll " +
      s"(value_upm float, pi float, aqi float, aqi_cn float)"
    val create_table = s"CREATE TABLE IF NOT EXISTS plume.pollution_data_by_geo " +
      s"(geo text, latitude double, longitude double, timestamp double, " +
      s"overall_data frozen <plume.poll>, nitrous_data frozen <plume.poll>, " +
      s"ozone_data frozen <plume.poll>, particulate_matter frozen <plume.poll>, " +
      s"particulate_matter_ten frozen <plume.poll>, " +
      s"particulate_matter_twenty_five frozen <plume.poll>," +
      s"PRIMARY KEY (geo, timestamp))"

    session.execute(create_type)
    session.execute(create_table)

    // Set number of decimals for latitude/longitude join
    val formatter = new DecimalFormat("#.#####")

    // Obtain geodatadictionary table from Cassandra, collect as MAP, and broadcast
    val geo: CassandraTableScanRDD[CassandraRow] = sc.cassandraTable("plume",
      "geodatadictionary")
    val geoMap = geo.map(row => ((formatter.format(row.getDouble("latitude")),
      formatter.format(row.getDouble("longitude"))), row.getString("geo")))
      .collectAsMap()
    val geoB = sc.broadcast(geoMap)

    // Kafka integration parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Stream from Kafka into Spark Streaming
    val topics = Array("plume_pollution")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // Join stream with geo and prepare RDD for Cassandra
    val pollution = stream.map(record => parseRecord(record.value))
      .map(x => (geoB.value.get(formatter.format(x(0)._1),
        formatter.format(x(0)._2)), x(0)))
      .map(x => (x._1.get, x._2._1, x._2._2, x._2._3,
        UDTValue.fromMap(x._2._4 + ("value_upm" -> None)),
        UDTValue.fromMap(x._2._5), UDTValue.fromMap(x._2._6),
        UDTValue.fromMap(x._2._7 + ("value_upm" -> None)),
        UDTValue.fromMap(x._2._8), UDTValue.fromMap(x._2._9)))

    // Write stream of RDDs to Cassandra
    pollution.foreachRDD(x => x.saveToCassandra("plume", "pollution_data_by_geo",
      SomeColumns("geo", "latitude", "longitude", "timestamp", "overall_data",
        "nitrous_data", "ozone_data", "particulate_matter",
        "particulate_matter_ten", "particulate_matter_twenty_five")))

    // Uncomment print to troubleshoot stream
    //pollution.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
