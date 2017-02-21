package fi.helsinki.cs.nodes.libubispark

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import fi.helsinki.cs.nodes.libubispark.rdd.RDD

import scala.reflect.ClassTag

class UbisparkContext(conf: UbisparkConf) {

  val startTime = System.currentTimeMillis

  /**
    * Create a SparkContext that loads settings from system properties (for instance, when
    * launching with ./bin/spark-submit).
    */
  def this() = this(new UbisparkConf())

  private val stopped: AtomicBoolean = new AtomicBoolean(false)

  /**
    * The active, fully-constructed SparkContext.  If no SparkContext is active, then this is `null`.
    *
    * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK.
    */
  private val activeContext: AtomicReference[UbisparkContext] =
  new AtomicReference[UbisparkContext](null)

  /**
    * Run a function on a given set of partitions in an RDD and pass the results to the given
    * handler function. This is the main entry point for all actions in Spark.
    */
  def runJob[T, U: ClassTag](
                              rdd: RDD[T],
                              func: Iterator[T] => U,
                              partitions: Seq[Int],
                              resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }

    // TODO: Not implemented
  }

  /**
    * Run a function on a given set of partitions in an RDD and return the results as an array.
    */
  def runJob[T, U: ClassTag](
                              rdd: RDD[T],
                              func: Iterator[T] => U,
                              partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  /**
    * Run a job on all partitions in an RDD and return the results in an array.
    */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  /**
    * Run a job on all partitions in an RDD and pass the results to a handler function.
    */
  def runJob[T, U: ClassTag](
                              rdd: RDD[T],
                              processPartition: Iterator[T] => U,
                              resultHandler: (Int, U) => Unit) {
    runJob[T, U](rdd, processPartition, 0 until rdd.partitions.length, resultHandler)
  }
}