package fi.helsinki.cs.nodes.libubispark

import java.util.concurrent.Callable

import java.util.concurrent.ForkJoinPool


/**
 * Created by lagerspe on 17.2.2017.
 */

@SerialVersionUID(1L)
case class LocalRunner(threads:Int = 1) {
  val pool = new ForkJoinPool(threads)

  def scheduleTask[T](task: Callable[T]) = {
    pool.submit(task)
  }

  def sqsum() = {
    Math.pow(Math.random(), 2)
  }
}
