package EasyJSON

import net.minidev.json.{JSONValue, JSONArray, JSONObject}
import scala.language.dynamics


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

//case class Exception extends Exception

class ScalaJSON(o: java.lang.Object) extends Seq[ScalaJSON] with Dynamic {
  override def toString: String = o.toString
  def toInt: Int = o match {
    case i: Integer => i
    case _ => throw new Exception
  }
  def toDouble: Double = o match {
    case d: java.lang.Double => d
    case f: java.lang.Float => f.toDouble
    case _ => throw new Exception
  }
  def apply(key: String): ScalaJSON = o match {
    case m: JSONObject => new ScalaJSON(m.get(key))
    case _ => throw new Exception
  }

  def apply(idx: Int): ScalaJSON = o match {
    case a: JSONArray => new ScalaJSON(a.get(idx))
    case _ => throw new Exception
  }
  def length: Int = o match {
    case a: JSONArray => a.size()
    case m: JSONObject => m.size()
  }
  def iterator: Iterator[ScalaJSON] = o match {
    case a: JSONArray => new ScalaJSONIterator(a.iterator())
    case _ => throw new Exception
  }

  def selectDynamic(name: String): ScalaJSON = apply(name)
  def applyDynamic(name: String)(arg: Any) = {
    arg match {
      case s: String => apply(name)(s)
      case n: Int => apply(name)(n)
      case u: Unit => apply(name)
    }
  }
}
