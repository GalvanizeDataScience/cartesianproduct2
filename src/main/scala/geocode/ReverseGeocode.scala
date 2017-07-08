package geocode

import scala.io._
import java.util.Properties

import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.SparkSession
import com.datastax.spark
import com.datastax.spark._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import scala.util.parsing.json.JSON.parseFull
import EasyJSON._
import EasyJSON.JSON.{makeJSON, parseJSON}
import EasyJSON.ScalaJSON
import EasyJSON.ScalaJSONIterator
import com.datastax.driver.core.Cluster


/**
  * Created by AnthonyAbercrombie on 7/6/17.
  */


object ReverseGeocode{

  // Define Spark configuration
  val conf = new SparkConf().setMaster("local[2]").setAppName("spark_conf")
  // Define Spark Context, should identify and connect to a locally running cassandra database
  val sc = new SparkContext(conf) //.set(“spark.cassandra.connection.host”, “localhost”)

  def main(args: Array[String]): Unit = {
    // dummy values for latitude and longitude
    val latitudeTest = 40.714224
    val longitudeTest = -73.961452

    // GOOGLEMAPS API key
    val GOOGLEMAPS = "AIzaSyBXmcsOVZ-d2B2aiVcwoUpSai-JNh3y5yc"

    // dummy result from google maps API
    val result = GoogleMapsRequester(latitudeTest, longitudeTest, GOOGLEMAPS, "locality")
    println(result)

    val coordinatesTest = QueryDistinctLatLongs()
    println(coordinatesTest)

    // Connect to the Cassandra DB
    implicit val session = new Cluster
    .Builder()
      .addContactPoints("localhost")
      .withPort(9042)
      .build()
      .connect()

    // Build Cassandra Assets
    val createKeyspace = "CREATE keyspace plume WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };"
    val createUDT = "CREATE TYPE plume.pollution_data (value_upm float,pi float,aqi float,aqi_cn float);"
    val buildRaw = "CREATE TABLE IF NOT EXISTS plume.pollution_data_by_lat_lon (latitude double,longitude double,timestamp double,pm_data frozen <pollution_data>,nitrous_data frozen <pollution_data>,pm_data_ten frozen <pollution_data>,pm_data_twenty_five frozen <pollution_data>,ozone_data frozen <pollution_data>,overall_data frozen <pollution_data>,primary key (latitude, longitude, timestamp));"
    val buildGeoDict = "CREATE TABLE IF NOT EXISTS plume.geodatadictionary (latitude double,longitude double,geo text,primary key (latitude, longitude));"

    session.execute(createKeyspace)
    session.execute(createUDT)
    session.execute(buildRaw)
    session.execute(buildGeoDict)

    Geocoder(coordinatesTest)
  }


//  // Define Spark configuration
//  val conf = new SparkConf().setMaster("local[2]").setAppName("spark_conf")
//  // Define Spark Context, should identify and connect to a locally running cassandra database
//  val sc = new SparkContext(conf) //.set(“spark.cassandra.connection.host”, “localhost”)


  /**
    * Executes a query that gets distinct lat/long pairs from the raw Plume Cassandra Table and returns a Sequence of
    * lat/long sets.
    */
  def QueryDistinctLatLongs() : Seq[Map[String, Double]] = {
    // Input : No input
    // OUTPUT : coordinates ---- Seq[Set]
    case class PlumeStationCoords(latitude: Double, longitude: Double)
    val rawGeoData: CassandraTableScanRDD[CassandraRow] = sc.cassandraTable("plume", "pollution_data_by_lat_lon").select("latitude", "longitude")

    val cols: Array[String] = Array("latitude", "longitude") // Array of column names of the same type
    val latLongArrays = rawGeoData.map(row => cols.map(row.getDouble(_)))

    val coordinates = latLongArrays.map(x => Map("latitude" -> x(0), "longitude" -> x(1))).collect()
  coordinates
  }


  /**
    * Applies the GoogleMapsRequester helper function to a Sequence of lat/long pairs.
    * Organizes the results into the GeoDictionary Cassandra Table, which is created on the spot. The table
    * is composed with the following columns:
    *
    * latitude : Double
    * longitude: Double
    * country: String
    * political: String
    * administrative_1 : String
    * geodata : UDT --- ask Daniel Gorham
    *
    */
  case class geodata(latitude: Double, longitude: Double, geodata: String)
  def Geocoder(coordinates: Seq[Map[String, Double]]) : Unit = {
    // Input : coordinates ---- Sequence of unique lat/long pairs generated by GetDistinctLatLongs()
    // Output: GeoDictionary ---- Cassandra Table that maps each unique lat long to geographic metadata

    // Get EasyJSON parsing objects after applying to GoogleMapRequest
    val resultsSeq = for (point <- coordinates) yield parseJSON(GoogleMapsRequester(point("latitude"), point("longitude"), "AIzaSyBXmcsOVZ-d2B2aiVcwoUpSai-JNh3y5yc", "locality"))
    // Get latitude, longitude, and geo result_type info from each EasyJSON tree.
    val geoSeq = for (tree <- resultsSeq) yield (tree.results(0).geometry.location.lat.toDouble, tree.results(0).geometry.location.lng.toDouble, tree.results(0).formatted_address.toString)
    // Parallelize geoSeq into RDD
    val geoRDD = sc.parallelize(geoSeq)

    // Save geoRDD to Cassandra.
    geoRDD.saveToCassandra("plume", "geodatadictionary", SomeColumns("latitude", "longitude", "geo"))
    println("saveToCassandra was successful!")

  }

  /**
    * Takes a latitude coord [lat], a longitude coord [long],
    * Google Maps API key [GOOGLEMAPS], and a results_type (e.g. locality). These arguements are then
    *  factored into an API call. The results of the API call are then
    *  returned as a json string [googleMapsResult]
    */
  def GoogleMapsRequester(lat: Double, long: Double, GOOGLEMAPS: String, result_type: String) : String = {
    // Input: lat --- latitude, long --- longitude, GOOGLEMAPS --- API key
    // Output: googleMapsResult --- raw result from google maps API

    // Format inputs into a url string that will be used to request from a REST API
    val requestURL = f"https://maps.googleapis.com/maps/api/geocode/json?latlng=$lat%s,$long%s&result_type=$result_type%s&key=$GOOGLEMAPS%s"
    // Make an HTTP GET request to the requestURL and return the response as a string
    val googleMapsResult = scala.io.Source.fromURL(requestURL).mkString
    googleMapsResult
  }
}


