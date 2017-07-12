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
import geocode.ReverseGeocode.GoogleMapsRequester
import EasyJSON.JSON.parseJSON


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
    // access google maps token https://github.com/zipfian/cartesianproduct2/wiki/TOKEN
    lazy val GOOGLEMAPS:Option[String] = sys.env.get("GOOGLEMAPS") orElse {
      println("No token found. Check how to set it up at https://github.com/zipfian/cartesianproduct2/wiki/TOKEN")
      None
    }

    // Setup Cassandra assets and environment
    val cassandraStatus = SetupCassandra()
    println("ASSERTING THAT CASSANDRA TABLES AND TYPES WERE SETUP")
    assert(cassandraStatus == true)


    // dummy result from google maps API
    val result = GoogleMapsRequester(latitudeTest, longitudeTest, GOOGLEMAPS, "locality")
    println("PRINTING AN EXAMPLE OF THE OBJECT RETURNED BY GOOGLE MAPS API")
    println(result)

    // Query raw pollution table from Cassandra
    val coordinatesTest = QueryDistinctLatLongs()
    println("PRINTING AN EXAMPLE OF THE DATA STRUCTURE USED BY THE 'Geocoder' FUNCTION")
    println(coordinatesTest)

    // Get geographic metadata from google maps API and store in plume.geodatadictionary table
    println("RUNNING THE GEOCODER ON COORDINATES EXTRACTED FROM THE 'plume.pollution_data_by_lat_lon' TABLE")
    Geocoder(coordinatesTest)
  }

  /**
    * Setup Cassandra Tables and clear the contents of plume.geodatadictionary so the table can be rebuilt.
    */
  def SetupCassandra(): Boolean = {
    // Connect to the Cassandra DB
    implicit val session = new Cluster
    .Builder()
      .addContactPoints("localhost")
      .withPort(9042)
      .build()
      .connect()

    // Build Cassandra Assets
    val createKeyspace = "CREATE keyspace IF NOT EXISTS plume WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    val createUDT = "CREATE TYPE IF NOT EXISTS plume.pollution_data (value_upm float,pi float,aqi float,aqi_cn float);"
    val buildRaw = "CREATE TABLE IF NOT EXISTS plume.pollution_data_by_lat_lon (latitude double,longitude double,timestamp double,pm_data frozen <pollution_data>,nitrous_data frozen <pollution_data>,pm_data_ten frozen <pollution_data>,pm_data_twenty_five frozen <pollution_data>,ozone_data frozen <pollution_data>,overall_data frozen <pollution_data>,primary key (latitude, longitude, timestamp));"
    val buildGeoDict = "CREATE TABLE IF NOT EXISTS plume.geodatadictionary (latitude double,longitude double,city text, state text,country text,geometry text,rawGeo text, primary key (latitude, longitude));"
    // Truncate plume.geodatadictionary so that it can be rebuilt
    val clearGeoDict = "TRUNCATE plume.geodatadictionary;"

    // Execute Cassandra commands
    session.execute(createKeyspace)
    session.execute(createUDT)
    session.execute(buildRaw)
    session.execute(buildGeoDict)
    session.execute(clearGeoDict)
    true
  }

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
    * city: String
    * state: String
    * country: String
    * geometry: String
    * rawgeo: String
    */
  case class geodata(latitude  : Double, longitude: Double, geodata: String)
  def Geocoder(coordinates: Seq[Map[String, Double]]) : Unit = {
    // Input : coordinates ---- Sequence of unique lat/long pairs generated by GetDistinctLatLongs()
    // Output: GeoDictionary ---- Cassandra Table that maps each unique lat long to geographic metadata

    // GOOGLEMAPS API key
    // access google maps token https://github.com/zipfian/cartesianproduct2/wiki/TOKEN
    lazy val GOOGLEMAPS:Option[String] = sys.env.get("GOOGLEMAPS") orElse {
      println("No token found. Check how to set it up at https://github.com/zipfian/cartesianproduct2/wiki/TOKEN")
      None
    }

//    // Get EasyJSON parsing objects after applying to GoogleMapRequest
    val resultsSeq = for (point <- coordinates) yield parseJSON(GoogleMapsRequester(point("latitude"), point("longitude"), GOOGLEMAPS, "locality|country|administrative_area_level_1"))

//    // Mine data from each EasyJSON tree.
    val geoSeq = for {
      (tree, i) <- resultsSeq.zipWithIndex
      latitude = coordinates(i)("latitude")
      longitude = coordinates(i)("longitude")
      result <- tree.results if result.types(0).toString == "locality"
      city = result.formatted_address.toString.split(",")(0)
      state = result.formatted_address.toString.split(",")(1)
      country = result.formatted_address.toString.split(",")(2)
      geometry = result.geometry.toString
      rawGeo = tree.toString
    } yield (latitude, longitude, city, state, country, geometry, rawGeo)

    // Parallelize geoSeq into RDD
    val geoRDD = sc.parallelize(geoSeq)

    // Save geoRDD to Cassandra
    geoRDD.saveToCassandra("plume", "geodatadictionary", SomeColumns("latitude", "longitude", "city", "state", "country", "geometry", "rawgeo"))
    println("saveToCassandra was successful!")
  }

  /**
    * Takes a latitude coord [lat], a longitude coord [long],
    * Google Maps API key [GOOGLEMAPS], and a results_type (e.g. locality). These arguements are then
    *  factored into an API call. The results of the API call are then
    *  returned as a json string [googleMapsResult]
    */
  def GoogleMapsRequester(lat: Double, long: Double, GOOGLEMAPS: Option[String], result_type: String) : String = {
    // Input: lat --- latitude, long --- longitude, GOOGLEMAPS --- API key
    // Output: googleMapsResult --- raw result from google maps API

    // Format inputs into a url string that will be used to request from a REST API
//    val requestURL = f"https://maps.googleapis.com/maps/api/geocode/json?latlng=$lat%s,$long%s&result_type=$result_type%s&key=${GOOGLEMAPS.get}%s"
    val requestURL = f"https://maps.googleapis.com/maps/api/geocode/json?latlng=$lat%s,$long%s&key=${GOOGLEMAPS.get}%s"
    // Make an HTTP GET request to the requestURL and return the response as a string
    val googleMapsResult = scala.io.Source.fromURL(requestURL).mkString
    googleMapsResult
  }
}


