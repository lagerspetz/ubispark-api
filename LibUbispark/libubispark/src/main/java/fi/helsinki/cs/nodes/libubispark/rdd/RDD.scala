package fi.helsinki.cs.nodes.libubispark.rdd

import fi.helsinki.cs.nodes.libubispark.{Partition, UbisparkContext, UbisparkException, Utils}

import scala.reflect.ClassTag

/**
  * Created by lagerspe on 21.2.2017.
  */
abstract class RDD[T: ClassTag](
                                 @transient private var _sc: UbisparkContext,
                                 @transient private var deps: Seq[Dependency[_]]
                                 ) extends Serializable {

  private def sc: UbisparkContext = {
    if (_sc == null) {
      throw new UbisparkException(
        "This RDD lacks a SparkContext. It could happen in the following cases: \n(1) RDD " +
          "transformations and actions are NOT invoked by the driver, but inside of other " +
          "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid " +
          "because the values transformation and count action cannot be performed inside of the " +
          "rdd1.map transformation. For more information, see SPARK-5063.\n(2) When a Spark " +
          "Streaming job recovers from checkpoint, this exception will be hit if a reference to " +
          "an RDD not defined by the streaming job is used in DStream operations. For more " +
          "information, See SPARK-13758.")
    }
    _sc
  }

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    */
  //@DeveloperApi
  def compute(split: Partition): Iterator[T]

  protected def getPartitions: Array[Partition]

  /**
    * Get the array of partitions of this RDD, taking into account whether the
    * RDD is checkpointed or not.
    */
  final def partitions: Array[Partition] = {
       val  partitions_ = getPartitions
        partitions_.zipWithIndex.foreach { case (partition, index) =>
          require(partition.index == index,
            s"partitions($index).partition == ${partition.index}, but it should equal $index")
        }
      partitions_
    }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  @transient private var doCheckpointCalled = false

  /**
    * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
    * has completed (therefore the RDD has been materialized and potentially stored in memory).
    * doCheckpoint() is called recursively on the parent RDDs.
    */
  def doCheckpoint(): Unit = {
  }

  def checkpoint(): Unit


  def localCheckpoint(): Unit
}

/**
  * :: DeveloperApi ::
  * Base class for dependencies.
  */
//@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}