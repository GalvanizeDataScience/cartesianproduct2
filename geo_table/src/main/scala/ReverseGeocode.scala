
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import EasyJSON.JSON.{parseJSON}
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
    val latitudeTest = 48.85
    val longitudeTest = 2.294

    // GOOGLEMAPS API key
    // access google maps token https://github.com/zipfian/cartesianproduct2/wiki/TOKEN
    lazy val GOOGLEMAPS:Option[String] = sys.env.get("GOOGLEMAPS") orElse {
      println("No token found. Check how to set it up at https://github.com/zipfian/cartesianproduct2/wiki/TOKEN")
      None
    }

    // Setup Cassandra assets and environment
    val cassandraStatus = SetupCassandra()
//    println("ASSERTING THAT CASSANDRA TABLES AND TYPES WERE SETUP")
    assert(cassandraStatus == true)


    // dummy result from google maps API
    val result = GoogleMapsRequester(latitudeTest, longitudeTest, GOOGLEMAPS, "locality")
//    println("PRINTING AN EXAMPLE OF THE OBJECT RETURNED BY GOOGLE MAPS API")
//    println(result)

    // Query raw pollution table from Cassandra
    val coordinatesTest = QueryDistinctLatLongs()
//    println("PRINTING AN EXAMPLE OF THE DATA STRUCTURE USED BY THE 'Geocoder' FUNCTION")
//    println(coordinatesTest)

    // Get geographic metadata from google maps API and store in plume.geodatadictionary table
//    println("RUNNING THE GEOCODER ON COORDINATES EXTRACTED FROM THE 'plume.pollution_data_by_lat_lon' TABLE")
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
    val buildGeoDict = "CREATE TABLE IF NOT EXISTS plume.geodatadictionary (latitude double,longitude double,city text, locality text,country text,geometry text,rawGeo text, primary key (latitude, longitude));"
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
    * locality: String
    * country: String
    * geometry: String
    * rawgeo: String
    */
  //  case class geodata(latitude  : Double, longitude: Double, geodata: String)
  case class geodictionary(latitude: Double, longitude: Double, city: String, locality: String,country: String,geometry: String,raw_geo: String)
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

//    println("PRINTING resultsSeq")
//    println(resultsSeq)

    val geoSeq3 = for {
      (tree, i) <- resultsSeq.zipWithIndex
      latitude = coordinates(i)("latitude")
      longitude = coordinates(i)("longitude")
      //      result <- tree.results if result.types(0).toString == "locality"
      result <- tree.results if result.types.toList.map(x => x.toString).contains("locality")
      city = result.formatted_address.toString.split(",").head
      locality = result.formatted_address.toString
      country = result.formatted_address.toString.split(",").last
      geometry = result.geometry.toString
      rawGeo = tree.toString
    } yield (latitude, longitude, city, locality, country, geometry, rawGeo)
//    println("PRINTING geoSeq3")
//    println(geoSeq3)


    // Parallelize geoSeq into RDD
    val geoRDD = sc.parallelize(geoSeq3)

    //geoRDD.take(3).foreach(println)

    // Save geoRDD to Cassandra
    geoRDD.saveToCassandra("plume", "geodatadictionary", SomeColumns("latitude", "longitude", "city", "locality", "country", "geometry", "rawgeo"))
//    println("saveToCassandra was successful!")
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