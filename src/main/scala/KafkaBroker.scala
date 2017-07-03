import java.util.Properties
import scala.io._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Kafka broker to ingest data from plume.io
  * /pollution/forecast
  * https://api.plume.io/1.0/pollution/forecast?token=xxx&lat=48.85&lon=2.294
  */

object KafkaBroker extends App {

  override def main(args: Array[String]): Unit = {

    // access plume token https://github.com/zipfian/cartesianproduct2/wiki/TOKEN
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

    // TODO: implement ProducerCallback()

    // query API on an infinity loop with 'sleepTime' intervals
    while (true){

      // create producer with 'props' properties
      val producer = new KafkaProducer[String, String](props)

      // query web API - response will be a String
      val response = Source.fromURL(
        "https://api.plume.io/1.0/pollution/forecast?token="+ token.get +"&lat="+ lat +"&lon="+ lon
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

  } // end of main

} // end of KafkaBroker object
