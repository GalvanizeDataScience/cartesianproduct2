package utils

/**
  * Created by daniel on 7/6/17.
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
