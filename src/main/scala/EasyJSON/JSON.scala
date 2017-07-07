package EasyJSON

import net.minidev.json.JSONValue
import net.minidev.json.JSONArray
import net.minidev.json.JSONObject

import scala.collection.JavaConversions._

/**
  * Credit for this code goes out to Julian Schrittwieser, who wrote a clever wrapper for the json-smart library that
  * returns neat results as such:
  *
  * val person_json = """{
  * "name": "Joe Doe",
  * "age": 45,
  * "kids": ["Frank", "Marta", "Joan"]
  ( }"""
  *
  * person.name.toString      // "Joe Doe"
  * person.kids(1).toString   // "Marta"
  */
object JSON {
  def parseJSON(s: String) = new ScalaJSON(JSONValue.parse(s))
  def makeJSON(a: Any): String = a match {
    case m: Map[String, Any] => m.map {
      case (name, content) => "\"" + name + "\":" + makeJSON(content)
    }.mkString("{", ",", "}")
    case l: List[Any] => l.map(makeJSON).mkString("[", ",", "]")
    case l: java.util.List[Any] => l.map(makeJSON).mkString("[", ",", "]")
    case s: String => "\"" + s + "\""
    case i: Int => i.toString
  }

  implicit def ScalaJSONToString(s: ScalaJSON) = s.toString
  implicit def ScalaJSONToInt(s: ScalaJSON) = s.toInt
  implicit def ScalaJSONToDouble(s: ScalaJSON) = s.toDouble
}
