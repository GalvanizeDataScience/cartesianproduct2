package EasyJSON

import net.minidev.json.{JSONValue, JSONArray, JSONObject}


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
class ScalaJSONIterator(i: java.util.Iterator[java.lang.Object]) extends Iterator[ScalaJSON] {
  def hasNext = i.hasNext()
  def next() = new ScalaJSON(i.next())
}
