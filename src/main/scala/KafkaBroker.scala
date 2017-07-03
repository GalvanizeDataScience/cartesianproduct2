import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.io._
import org.json4s._
import org.json4s.native.JsonMethods._
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

    // access plume API
    lazy val token:Option[String] = sys.env.get("PLUMETOKEN") orElse {
      println("No token found. Check how to set it up at https://github.com/zipfian/cartesianproduct2/wiki/TOKEN")
      None
    }

    // Spark Streaming is now connected to Apache Kafka and consumes messages every 10 seconds
//    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaProducer")
//    val ssc = new StreamingContext(conf, Seconds(10))

//    val events = args(0).toInt
    val topic = args(0) // plume
    val brokers = args(1) // "broker1:port,broker2:port"
    val sleepTime = args(2) // time between queries to API
    val lat = 48.85
    val lon = 2.294

    val rnd = new Random()

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
//    props.put("metadata.broker.list", "192.168.146.132:9092, 192.168.146.132:9093, 192.168.146.132:9094");
    props.put("client.id", "ScalaProducer") // 0, 1, 2 in tutorial
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("retries", new Integer(1))
//    props.put("batch.size", new Integer(16384))
//    props.put("linger.ms", new Integer(1))
//    props.put("buffer.memory", new Integer(133554432))

    // create producer with 'props' properties
    val producer = new KafkaProducer[String, String](props)


//    Sample code to publish to producer
//    val t = System.currentTimeMillis()
//
//    for (nEvents <- Range(0, events)) {
//      val runtime = new Date().getTime()
//      val ip = "192.168.2." + rnd.nextInt(255)
//      val msg = runtime + "," + nEvents + ",www.example.com," + ip
//      val data = new ProducerRecord[String, String](topic, ip, msg)
//
//      //async
//      //producer.send(data, (m,e) => {})
//      //sync
//      producer.send(data)
//
//    }

    // case class for pollution data
//    case class LabeledContent(label: Double, content: Array[String])

    // name of
//    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer-group", Map("plume_pollution" -> 5))

//    kafkaStream.print()

    // query API on an infinity loop
    while (true){

      // query web API - response will be a String
      val response = Source.fromURL(
        "https://api.plume.io/1.0/pollution/forecast?token="+ token +"&lat="+ lat +"&lon="+ lon
      ).mkString


      // process String as JSON
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
      val recordMetadata = producer.send(producerRecord);

      // pause in between queries - this should be an argument
      Thread.sleep(5000)

    }

    producer.close() // this might  not run but placeholder in case loop changes

//    ssc.start

  }

}
