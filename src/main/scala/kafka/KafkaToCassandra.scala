package kafka

import java.util
import java.util.Properties

import com.datastax.driver.core.Cluster
import local_utils.{DoubleExtractor, ListExtractor, MapExtractor}
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON
/**
  * Created by michaelseeber on 7/11/17.
  */
object KafkaToCassandra {
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

    val TOPIC = "plume_pollution"
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(TOPIC))

    implicit val session = new Cluster
      .Builder()
      .addContactPoints("localhost")
      .withPort(9042)
      .build()
      .connect()

    while(true) {
      val records = consumer.poll(25)
      for (record <- records.asScala) {
        val result_tuple = parseRecord(record.value()).head
        val latitude = result_tuple._1
        val longitude = result_tuple._2
        val timestamp = result_tuple._3

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
