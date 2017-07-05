/**
  * Created by AnthonyAbercrombie on 7/5/17.
  */

import scala.util.parsing.json.JSONObject



def KafkaParser(kafkaLog : String) : Seq[String] = {
  // Parses a Kafka log [kafkaLog] and returns a sequence of strings with json
  // structure [plumeRecords]
}

def PlumeParser(plumeRecords : Seq[String]) : Seq[CassandraRow] = {
  // Parses a sequence of strings with json structure [plumeRecords], extracts
  // geographically relevant data, and returns a sequence of Cassandra
  // rows for a geographically enriched Cassandra table [plumeGeoRows].
}

def GoogleMapsRequester(lat: Double, long: Double, GOOGLEMAPS: String) : String = {
// Takes a latitude coord [lat], a longitude coord [long], and a
// Google Maps API key [GOOGLEMAPS]. These arguements are then
// factored into an API call. The results of the API call are then
// returned as a json string [googleMapsResult]
  val base_url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng=$lat%f,$long%f&key=$GOOGLEMAPS%s"


  println(f"$name%s is $height%2.2f meters tall")