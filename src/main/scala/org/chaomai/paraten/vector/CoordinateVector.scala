package org.chaomai.paraten.vector

import breeze.linalg.DenseVector
import breeze.stats.distributions.Rand
import breeze.storage.Zero
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chaomai.paraten.support.CanUse
import org.chaomai.paraten.tensor.VEntry

import scala.reflect.ClassTag

/**
  * Created by chaomai on 02/05/2017.
  */
class CoordinateVector[
    @specialized(Double, Float, Int, Long) V: ClassTag: CanUse](
    val size: Int,
    private val storage: RDD[VEntry[V]])
    extends Vector {

  val shape: IndexedSeq[Int] = IndexedSeq(size)
  val dimension: Int = 1

  def foreach(f: VEntry[V] => Unit): Unit = storage.foreach(f)

  def mapPartitions[U: ClassTag](
      f: Iterator[VEntry[V]] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] =
    storage.mapPartitions(f, preservesPartitioning)

  def apply(idx: Int): V =
    storage
      .filter(e => e.coordinate == idx)
      .take(1)(0)
      .value

  override def toString: String = {
    storage
      .collect()
      .foldLeft("[")((acc, e) =>
        (acc, e) match {
          case ("[", entry) => acc + entry.toString
          case _ => acc + ", " + e.toString
      }) + "]"
  }
}

object CoordinateVector {
  def rand[@specialized(Double) V: ClassTag: Zero: CanUse](size: Int,
                                                           rand: Rand[V] =
                                                             Rand.uniform)(
      implicit sc: SparkContext): CoordinateVector[V] = {
    val v = DenseVector.rand[V](size, rand)

    val entries = for {
      i <- 0 until size
    } yield VEntry(i, v(i))

    CoordinateVector(size, sc.parallelize(entries))
  }

  def vals[@specialized(Double, Float, Int, Long) V: ClassTag: CanUse](vs: V*)(
      implicit sc: SparkContext): CoordinateVector[V] = {
    CoordinateVector(vs.length,
                     sc.parallelize(
                       vs.zipWithIndex
                         .map(p => VEntry(p._2, p._1))
                         .filter(e => e.value != 0)))
  }

  def fromRDD[@specialized(Double, Float, Int, Long) V: ClassTag: CanUse](
      size: Int,
      rdd: RDD[VEntry[V]]): CoordinateVector[V] = {
    CoordinateVector(size, rdd)
  }

  def apply[V: ClassTag: CanUse](size: Int,
                                 rdd: RDD[VEntry[V]]): CoordinateVector[V] =
    new CoordinateVector[V](size, rdd)
}
