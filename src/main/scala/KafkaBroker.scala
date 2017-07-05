package Kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io._

/**
  * Kafka broker to ingest data from plume.io
  * /pollution/forecast
  * https://api.plume.io/1.0/pollution/forecast?token=xxx&lat=48.85&lon=2.294
  */

object KafkaBroker extends App {


  case class Coordinates(lat: Double, lon: Double)
jsdlk
  override def main(args: Array[String]): Unit = {

    // parameters
    val topic = args(0) // plume_pollution
    val brokers = args(1) // localhost:9092 - "broker1:port,broker2:port"
    val lat = args(2).toDouble // latitude - test value: 48.85
    val lon = args(3).toDouble // longitude - test value: 2.294
    val sleepTime = args(4).toInt // 1000 - time between queries to API

    // user 'lat' and 'lon' to create Coordinates object
    val location = Coordinates(lat, lon)

    startIngestion(brokers, topic, location, sleepTime)


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
