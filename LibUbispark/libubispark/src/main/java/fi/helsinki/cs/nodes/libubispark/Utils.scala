package fi.helsinki.cs.nodes.libubispark

import collection.JavaConverters._

/**
  * Created by lagerspe on 21.2.2017.
  */
object Utils {
  /**
    * Counts the number of elements of an iterator using a while loop rather than calling
    * [[scala.collection.Iterator#size]] because it uses a for loop, which is slightly slower
    * in the current version of Scala.
    */
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }


  /**
    * Returns the system properties map that is thread-safe to iterator over. It gets the
    * properties which have been set explicitly, as well as those for which only a default value
    * has been defined.
    */
  def getSystemProperties: Map[String, String] = {
    System.getProperties.stringPropertyNames().asScala
      .map(key => (key, System.getProperty(key))).toMap
  }
}
