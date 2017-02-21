package fi.helsinki.cs.nodes.libubispark

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
  * Created by lagerspe on 21.2.2017.
  */
@SerialVersionUID(1L)
class UbisparkConf(loadDefaults:Boolean) extends Cloneable with Serializable{

  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private def loadFromSystemProperties(silent: Boolean): UbisparkConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): UbisparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }

    settings.put(key, value)
    this
  }
}
