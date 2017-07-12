package geocode

/**
  * Created by michaelseeber on 7/10/17.
  */
import scala.util.parsing.json._
import scala.io.Source

object PollutionDataCities extends App{

  override def main(args: Array[String]): Unit = {
    val poll = new pollutionGetter
    println(poll.main())

  }

  case class Coordinates(lat: Double, lon: Double)

  class pollutionGetter {

    def main(): Unit = {

      val inputCities = "utils/400cities_final.txt"
      val splitCities = Source.fromFile(inputCities).getLines.mkString.split(",")

      coordinateGetter(splitCities)


    }

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
      lazy val GOOGLEMAPS: Option[String] = sys.env.get("GOOGLEMAPS") orElse {
        println("No token found.")
        None
      }
      lazy val GmapsURLroot = "https://maps.googleapis.com/maps/api/geocode/json?address="

      def latLng(address: String): Coordinates = {
        val response = Source.fromURL(GmapsURLroot + address + "&key=" + GOOGLEMAPS.get).mkString
        val json = JSON.parseFull(response)
        val lat = getCoord(json, "lat")
        val lng = getCoord(json, "lng")
        val coords = new Coordinates(lat, lng)
        coords
      }

      addresses.map(x => (x, latLng(x)))
    }
  }

}


