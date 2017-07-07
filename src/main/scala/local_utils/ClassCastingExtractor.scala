package local_utils

/**
  * Created by daniel on 7/6/17.
  */

/*
Utilities to cast objects to various datatypes. Useful for parsing JSON from strings using only Scala's
built in JSON parser, which only returns a String -> String map.

See here: https://stackoverflow.com/questions/4170949/how-to-parse-json-in-scala-using-standard-scala-classes
 */

class ClassCastingExtractor[T] {
  def unapply(a: Any): Option[T] = {
    Some(a.asInstanceOf[T])
  }
}

object MapExtractor extends ClassCastingExtractor[Map[String, Any]]
object ListExtractor extends ClassCastingExtractor[List[Any]]
object StringExtractor extends ClassCastingExtractor[String]
object DoubleExtractor extends ClassCastingExtractor[Double]
