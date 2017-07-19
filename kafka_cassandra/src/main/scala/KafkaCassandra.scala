/**
  * Created by Daniel Gorham
  * Edited by Michael Seeber on 7/11/17.
  * Edited by Anthony Abercrombie on 7/18/17.
  */
import java.util
import java.util.Properties

import com.datastax.driver.core.Cluster
import ClassCasting.{DoubleExtractor, ListExtractor, MapExtractor}
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON


object KafkaCassandra{
  def parseRecord(record: String) = {
    for {
      Some(MapExtractor(map)) <- List(JSON.parseFull(record))
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

    } yield { (latitude,
      longitude,
      timestamp,
      particulate_matter,
      nitrous_oxide,
      particulate_matter_ten,
      particulate_matter_twenty_five,
      ozone,
      overall)
    }
  }

  def buildCassandraUdtFromMap(mapInput: Map[String, Any]): String = {
    var res: List[String] = List()
    for ((k,v) <- mapInput) {
      val here = s"$k: $v"
      res = res ::: List(here)
    }
    "{" + res.mkString(", ") + "}"
  }

  def main(args: Array[String]): Unit = {
    // SETUP CONSUMER THAT WILL SUBSCRIBE TO THE plume_pollution TOPIC
    // ON THE KAFKA CLUSTER datastax-enterprise-5
    val TOPIC = "plume_pollution"
    val props = new Properties()
    // Specify the Zookeeper ports of each of the instances in the Kafka cluster
    // Only one Zookeper IP:port is needed, providing a list just in case one instance goes down
    props.put("zoookeeper.connect", "35.190.179.65:2181,35.185.26.73:2181,35.190.148.28:2181")
    // Specify the Broker ports of each instance. Assumes that there is one broker on each port
    props.put("bootstrap.servers", "35.190.179.65:9092,35.185.26.73:9092,35.190.148.28:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(TOPIC))

    // CREATE CASSANDRA CLUSTER SESSION
    implicit val session = new Cluster
    .Builder()
      .addContactPoints("localhost")
      .withPort(9042)
      .build()
      .connect()

    // CREATE UDT AND ROOT CASSANDRA TABLE
    val create_type = s"CREATE TYPE IF NOT EXISTS plume.pollution_data (" +
      s"value_upm float, pi float, aqi float, aqi_cn float)"

    // Had issues with timestamp as a double.
    // After looking at an example "timestamp":1499047200" bigint seems more appropriate
    val create_table = s"CREATE TABLE IF NOT EXISTS plume.pollution_data_by_lat_lon (" +
      s"latitude double, longitude double, timestamp bigint, pm_data frozen <pollution_data>," +
      s"nitrous_data frozen <pollution_data>, pm_data_ten frozen <pollution_data>," +
      s"pm_data_twenty_five frozen <pollution_data>, ozone_data frozen <pollution_data>," +
      s"overall_data frozen <pollution_data>, primary key ((latitude, longitude), timestamp)" +
      s") with clustering order by (timestamp desc)"

    session.execute(create_type)
    session.execute(create_table)

    while(true) {
      val records = consumer.poll(25)
      for (record <- records.asScala) {
        val result_tuple = parseRecord(record.value()).head
        val latitude = result_tuple._1
        val longitude = result_tuple._2
        val timestamp = result_tuple._3

        // TESTING KAFKA CONSUMER-PRODUCER CONNECTION
        println(latitude)
        println(longitude)
        println(timestamp)

        // Parse the Maps that come out of the JSON into the proper format for Cassandra insertion
        val pm_data = buildCassandraUdtFromMap(result_tuple._4)
        val no_data = buildCassandraUdtFromMap(result_tuple._5)
        val pm_data_ten = buildCassandraUdtFromMap(result_tuple._6)
        val pm_data_twenty_five = buildCassandraUdtFromMap(result_tuple._7)
        val ozone_data = buildCassandraUdtFromMap(result_tuple._8)
        val overall_data = buildCassandraUdtFromMap(result_tuple._9)

        // Build CQL command to insert data
        val command = s"INSERT INTO plume.pollution_data_by_lat_lon (latitude, longitude, timestamp, " +
          s"nitrous_data, overall_data, ozone_data, pm_data, pm_data_ten, pm_data_twenty_five) values " +
          s"($latitude, $longitude, $timestamp, $no_data, $overall_data, $ozone_data, $pm_data, " +
          s"$pm_data_ten, $pm_data_twenty_five)"

        session.execute(command)

      }
    }
  }
}