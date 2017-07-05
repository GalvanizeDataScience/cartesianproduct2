import scala.util.parsing.json.JSONObject

def googleMapsApiCall(latlng : String, key : String) : JSONObject = {

  val latlngExample = '40.714224,-73.961452'
  val keyExample = "AIzaSyAuhZTKFnkgQcqq1tqdCNRsjQnHO5jZaU0"
  val base_url = "https://maps.googleapis.com/maps/api/geocode/json?latlng=40.714224,-73.961452&key=YOUR_API_KEY"

}