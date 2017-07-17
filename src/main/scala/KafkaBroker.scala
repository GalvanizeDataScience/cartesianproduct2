import java.util
import java.util.Properties
import java.io.File
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io._
import akka.actor._
import akka.routing.BalancingPool

/**
  * Kafka broker to ingest data from plume.io
  * /pollution/forecast
  * https://api.plume.io/1.0/pollution/forecast?token=xxx&lat=48.85&lon=2.294
  *
  * With an akka wrapper added to manage distribution of brokers to all >400 cities of the Plume.io dataset
  */

object KafkaBroker extends App {

  case class Coordinates(lat: Double, lon: Double)

  //class for sending Kafka parameters via akka PlumeApiActor actors
  case class Ingestion_parameters(brokers: String, topic: String, lat: Double, lon: Double, sleepTime: Int)

  class PlumeApiActor extends Actor {
    def receive = {
      case Ingestion_parameters(brokers, topic, lat, lon, sleepTime) => {

        // user 'lat' and 'lon' to create Coordinates object
        val location = Coordinates(lat, lon)
        println(s"this is the location, received by the actor: $location")

        startIngestion(brokers, topic, location, sleepTime)
      }
      // TODO: add a case for returning the name of the city associated with the lat/lon
      case _ => println("Not a geocoordinate")
    }
  }

  override def main(args: Array[String]): Unit = {

    // parameters
    val topic = args(0) // plume_pollution
    val brokers = args(1) // localhost:9092 - "broker1:port,broker2:port"
    val sleepTime = args(2).toInt // 1000 - time between queries to API

    //in current form, the lines below gets lat and lon from a test txt file;
    val testFileName = "test_latlons.txt"
    val testFileLoc = new File(getClass.getClassLoader.getResource(testFileName).getPath)

    val source = Source.fromFile(testFileLoc)
    val lines = source.getLines

    //getting a second iterator, just to get it's length (there's probably a way to do this with a single iterator)
    val source_forlen = Source.fromFile(testFileLoc)
    val lines_forlen = source_forlen.getLines

    val rm = "()".toSet


    //if line strings are in the form (city,(1.0,2.0)), and want to keep this but as (String, (Double,Double))
    val coords = lines.map(l => l.split(",")).map(a =>(a(0).filterNot(rm),(a(1).filterNot(rm).toDouble,a(2).filterNot(rm).toDouble)))

    //getting a second iterator, just to get it's length (there's probably a way to do this with a single iterator)
    val coords_forlen = lines_forlen.map(l => l.split(",")).map(a =>(a(0).filterNot(rm),(a(1).filterNot(rm).toDouble,a(2).filterNot(rm).toDouble)))

    val system = ActorSystem("PlumeActorSystem")

    /* The router BalancingPool utility automatically distributes akka actors, as they are instantiated, in a balanced
    manner across available nodes (here indicated as 4, when running locally).
    Later, for use on the Google Compute Engine, define router info in a config file*/

    val router = system.actorOf(BalancingPool(4).props(Props[PlumeApiActor]), "PlumeActorPool")

    val length = coords_forlen.length
    val lengthInt = length.toInt

    for (i <- 0 until lengthInt) {

      val loc = coords.next
      val lat = loc._2._1
      val lon = loc._2._2

      val ingestion_params = Ingestion_parameters(brokers, topic, lat, lon, sleepTime)
      router ! ingestion_params //send the router a message, which it will distribute to a sub-actor
    }
  } // end of main

  /**
    * Helper function to create a KafkaProducer using brokers ip and port
    *
    * @param brokers Broker information in the format 'localhost:9092'
    *                or "broker1:port,broker2:port"
    *
    * @return KafkaProducer[String, String]
    */

  def startBroker(brokers:String): KafkaProducer[String, String] = {

    // Kafka Broker properties
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaKafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("retries", new Integer(1))
    //    props.put("batch.size", new Integer(16384))
    //    props.put("linger.ms", new Integer(1))
    //    props.put("buffer.memory", new Integer(133554432))

    // TODO: implement ProducerCallback()

    new KafkaProducer[String, String](props)

  } // end of startBroker

  /**
    * Queries plume pollution API for a particular 'location' (lat, long) in an interval defined by 'sleepTime'
    * and creates a KafkaProducer to ingest content
    *
    * @param brokers Broker information in the format 'localhost:9092'
    *                or "broker1:port,broker2:port"
    * @param topic Topic to publish message to
    * @param location Latitude and Longitude to query pollution
    * @param sleepTime Time interval between queries to plume API
    *
    */


  def startIngestion(brokers:String, topic:String, location: Coordinates, sleepTime: Int) = {

    // access plume token https://github.com/zipfian/cartesianproduct2/wiki/TOKEN
    lazy val token:Option[String] = sys.env.get("PLUMETOKEN") orElse {
      println("No token found. Check how to set it up at https://github.com/zipfian/cartesianproduct2/wiki/TOKEN")
      None
    }

    while (true){

      // create producer with 'props' properties
      val producer = startBroker(brokers)

      // query web API - response will be a String
      val response = Source.fromURL(
        "https://api.plume.io/1.0/pollution/forecast?token="+ token.get +"&lat="+ location.lat +"&lon="+ location.lon
      ).mkString

      val producerRecord = new ProducerRecord[String, String](topic, response)
      val recordMetadata = producer.send(producerRecord)

      val meta = recordMetadata.get() // I could use this to write some tests
      val msgLog =
        s"""
           |topic     = ${meta.topic()}
           |offset    = ${meta.offset()}
           |partition = ${meta.partition()}
          """.stripMargin
      println(msgLog)

      producer.close()

      // pause in between queries - this should be an argument
      Thread.sleep(sleepTime)

    } // end of infinity loop


  } // end of startIngestion

} // end of KafkaBroker object
