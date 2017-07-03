import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.io._
import org.json4s._
import org.json4s.native.JsonMethods._


import scala.util.Random

/**
  * Kafka broker to ingest data from plume.io
  * /pollution/forecast
  * https://api.plume.io/1.0/pollution/forecast?token=xxx&lat=48.85&lon=2.294
  */

object KafkaBroker extends App {

  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")

    // Spark Streaming is now connected to Apache Kafka and consumes messages every 10 seconds
    val ssc = new StreamingContext(conf, Seconds(10))


    val events = args(0).toInt
    val topic = args(1)
    val brokers = args(2) // "broker1:port,broker2:port"
    val rnd = new Random()

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducer") // 0, 1, 2 in tutorial
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("retries", new Integer(1))
//    props.put("batch.size", new Integer(16384))
//    props.put("linger.ms", new Integer(1))
//    props.put("buffer.memory", new Integer(133554432))

    val producer = new KafkaProducer[String, String](props)


    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String](topic, ip, msg)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      producer.send(data)

    }

    val producerRecord = new ProducerRecord[String, String](topic, data)
    val recordMetadata = producer.send(producerRecord);


    // need to take token out of request


    // query web API - response will be a String
    val response = Source.fromURL(
      "https://api.plume.io/1.0/pollution/forecast?token=xxx&lat=48.85&lon=2.294"
    ).mkString

    // process String as JSON
    val jsonResponse = parse(response)

    // Go into the JObject instance and bind the list of fields to the constant fields
    val JObject(fields) = jsonResponse

    val firstField = fields.head
    val JField(key, JString(value)) = firstField

    // Extracting fields using XPath
    val JString(loginName) = jsonResponse \ "login"


    // name of
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer-group", Map("plume_pollution" -> 5))

    kafkaStream.print()

    ssc.start

  }
}
