package org.chaomai.paratd.vector

import breeze.linalg.{DenseVector, SparseVector, VectorBuilder}
import breeze.math.Semiring
import org.chaomai.paratd.support.CanUse
import org.chaomai.paratd.tensor.VEntry

import scala.reflect.ClassTag

/**
  * Created by chaomai on 03/05/2017.
  */
class LocalCoordinateVector[
    @specialized(Double, Float, Int, Long) V: ClassTag: Semiring: CanUse](
    val size: Int,
    private val storage: IndexedSeq[VEntry[V]])
    extends Vector {

  def head: VEntry[V] = storage.head

  def toDenseVector: DenseVector[V] = toSparseVector.toDenseVector

  def toSparseVector: SparseVector[V] = {
    val builder = new VectorBuilder[V](size)
    storage.foreach(e => builder.add(e.coordinate, e.value))
    builder.toSparseVector
  }

  /***
    * Inner product.
    *
    * @param y  another vector.
    * @param n  implicit Numeric.
    * @return   inner product.
    */
  def >*(y: CoordinateVector[V])(implicit n: Numeric[V]): V = {
    require(size == y.size,
            s"Requires inner product, "
              + s"but got v1.size = $size and v2.size = ${y.size}")

    val partitionFunc = (iter: Iterator[VEntry[V]]) => {
      iter.map { x =>
        val find = storage.find(e => e.coordinate == x.coordinate)
        find.map(e => n.times(x.value, e.value)).getOrElse(n.zero)
      }
    }

    y.mapPartitions(partitionFunc)
      .fold(n.zero)((e1, e2) => n.plus(e1, e2))
  }

  /***
    * Inner product.
    *
    * @param y  another vector.
    * @param n  implicit Numeric.
    * @return   inner product.
    */
  def >*(y: LocalCoordinateVector[V])(implicit n: Numeric[V]): V = {
    require(size == y.size,
            s"Requires inner product, "
              + s"but got v1.size = $size and v2.size = ${y.size}")

    val paired = for {
      a <- storage
      b <- y.storage
      if a.coordinate == b.coordinate
    } yield (a.value, b.value)

    paired.foldLeft(n.zero)((acc, p) => n.plus(acc, n.times(p._1, p._2)))
  }

  def apply(idx: Int): V =
    storage
      .filter(e => e.coordinate == idx)
      .head
      .value

  override def toString: String = {
    storage.foldLeft("[")((acc, e) =>
      (acc, e) match {
        case ("[", entry) => acc + entry.toString
        case _ => acc + ", " + e.toString
    }) + "]"
  }
}

object LocalCoordinateVector {
  def vals[V: ClassTag: Semiring: CanUse](vs: V*): LocalCoordinateVector[V] =
    LocalCoordinateVector(vs.size,
                          vs.zipWithIndex
                            .map(p => VEntry(p._2, p._1))
                            .filter(e => e.value != 0)
                            .toIndexedSeq)

  def apply[V: ClassTag: Semiring: CanUse](
      size: Int,
      vec: IndexedSeq[VEntry[V]]): LocalCoordinateVector[V] =
    new LocalCoordinateVector[V](size, vec)
}
