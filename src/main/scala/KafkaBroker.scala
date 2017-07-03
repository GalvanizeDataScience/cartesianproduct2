import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.io._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.concurrent.Future
//import org.json4s.jackson.JsonMethods._
// https://forums.databricks.com/questions/6928/cannot-parse-json-using-json4s-on-databricks.html


import scala.util.Random

/**
  * Kafka broker to ingest data from plume.io
  * /pollution/forecast
  * https://api.plume.io/1.0/pollution/forecast?token=xxx&lat=48.85&lon=2.294
  */

object KafkaBroker extends App {

  override def main(args: Array[String]): Unit = {

    // access plume token
    lazy val token:Option[String] = sys.env.get("PLUMETOKEN") orElse {
      println("No token found. Check how to set it up at https://github.com/zipfian/cartesianproduct2/wiki/TOKEN")
      None
    }

    // parameters
    val topic = args(0) // plume_pollution
    val brokers = args(1) // localhost:9092 - "broker1:port,broker2:port"
    val lat = args(2).toDouble // latitude - test value: 48.85
    val lon = args(3).toDouble // longitude - test value: 2.294
    val sleepTime = args(4).toInt // 1000 - time between queries to API

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

    // query API on an infinity loop with 'sleepTime' intervals

    // TODO: implement ProducerCallback()

    while (true){

      // create producer with 'props' properties
      val producer = new KafkaProducer[String, String](props)

      // query web API - response will be a String
      val response = Source.fromURL(
        "https://api.plume.io/1.0/pollution/forecast?token="+ token.get +"&lat="+ lat +"&lon="+ lon
      ).mkString


      // decided to keep the response as text but below is the code to further process the String as JSON
//      val jsonResponse = parse(response)

      // Go into the JObject instance and bind the list of fields to the constant fields
//      val JObject(fields) = jsonResponse

      //    val firstField = fields.head
      //    val JField(key, JDouble(value)) = firstField
      //      key: String = latitude
      //      value: Double = 48.85

      // Extracting fields using XPath
      //    val JDouble(longitude) = jsonResponse \ "longitude"
      //      longitude: Double = 2.294

      val producerRecord = new ProducerRecord[String, String](topic, response)
      val recordMetadata = producer.send(producerRecord)

      val meta = recordMetadata.get() // I could use this to write tests
      val msgLog =
        s"""
           |offset    = ${meta.offset()}
           |partition = ${meta.partition()}
           |topic     = ${meta.topic()}
          """.stripMargin
      println(msgLog)

      producer.close()

      // pause in between queries - this should be an argument
      Thread.sleep(sleepTime)

    } // end of infinity loop

  } // end of main

} // end of KafkaBroker object
