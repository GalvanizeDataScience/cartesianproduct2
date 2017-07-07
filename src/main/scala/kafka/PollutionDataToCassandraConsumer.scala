package kafka

import java.util.{Collections, Properties}
import java.util.concurrent.Executors

import scala.collection.JavaConversions._
import scala.util.parsing.json._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import com.datastax.driver.core.Cluster
import local_utils.{MapExtractor, ListExtractor, DoubleExtractor}

/*
Kafka consumer that dumps data directly to Cassandra with no preprocessing.
 */

object PollutionDataToCassandraConsumer {
  def main(args: Array[String]): Unit = {
    // Extract command line arguments
    val topic = args(0)
    val brokers = args(1)
    val groupId = args(2)
    val props = createProps(brokers, groupId)
    val consumer = new KafkaConsumer[String, String](props)
    run(consumer, topic, groupId)
  }

  // Create properties for the Kafka consumer
  def createProps(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  // Parse JSON API record into the proper datatypes
  // Doubles for latitude, longitude, and timestamp
  // Maps for the user defined types
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

  // Builds the proper input for a Cassandra user defined type from a Scala map
  // e.g. Map("one" -> "two", "three" -> "four") returns the string "{one: two, three: four}"
  def buildCassandraUdtFromMap(mapInput: Map[String, Any]): String = {
    var res: List[String] = List()
    for ((k,v) <- mapInput) {
      val here = s"$k: $v"
      res = res ::: List(here)
    }
    "{" + res.mkString(", ") + "}"
  }

  // Runs the Kafka consumer
  def run(consumer: KafkaConsumer[String, String], topic: String, groupId: String): Unit = {
    consumer.subscribe(Collections.singletonList(topic))

    // Connect to the Cassandra DB
    implicit val session = new Cluster
                               .Builder()
                               .addContactPoints("localhost")
                               .withPort(9042)
                               .build()
                               .connect()

    val poller = new Runnable {
      override def run(): Unit = {
        while (true) {
          // Pull 44 records at a time
          val records = consumer.poll(50)

          for (record <- records) {
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

          // Wait 5 seconds before polling Kafka again.
          Thread.sleep(5000)
        }
      }
    }
    Executors.newSingleThreadExecutor.execute(poller)
  }
}
