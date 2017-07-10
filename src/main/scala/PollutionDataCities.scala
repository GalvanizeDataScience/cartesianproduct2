import KafkaBroker.Coordinates
import scala.util.parsing.json._
import scala.io.Source

/**
  * Created by victorvulovic on 7/5/17.
  */
class PollutionDataCities {

  def main(): Unit = {

    val inputCities = "/Users/victorvulovic/400cities_final.txt"
    val splitCities = Source.fromFile(inputCities).getLines.mkString.split(",")

    coordinateGetter(splitCities)

  }

  /**
    * sample relevant JSON for the getCoord function below:
    * {
    *    "results" : [ ...
    *       { ...
    *         "geometry" : {
    *              "location" : {
    *                  "lat" : 37.4224764,
    *                  "lng" : -122.0842499
    *        }, ...
    *        }
    *      ],
    *      "status": "OK"
    * }
    */

  def getCoord(json_string: Option[Any], coordType: String): Double = {
    json_string match {
      case Some(m: Map[String, Any]) => m("results") match {
        case li: List[Map[String, Any]] => li.head match {
          case l: Map[String, Any] => l("geometry") match {
            case g: Map[String, Any] => g("location") match {
              case loc: Map[String, Any] => loc(coordType) match {
                case ct: Double => ct
              }
            }
          }
        }
      }
    }
  }

  def coordinateGetter(addresses: Array[String]): Array[Tuple2[String, Coordinates]] = {
    lazy val GmapsAPIkey:Option[String] = sys.env.get("GMAPS") orElse {
      println("No token found.")
      None
    }
    lazy val GmapsURLroot = "https://maps.googleapis.com/maps/api/geocode/json?address="

    def latLng(address: String): Coordinates = {
      val response = Source.fromURL(GmapsURLroot + address + "&key=" + GmapsAPIkey.get).mkString
      val json = JSON.parseFull(response)
      val lat = getCoord(json, "lat")
      val lng = getCoord(json, "lng")
      Coordinates(lat, lng)
    }
    addresses.map(x => (x, latLng(x)))
  }
}