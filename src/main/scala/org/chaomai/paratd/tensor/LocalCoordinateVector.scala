package org.chaomai.paratd.tensor

import breeze.linalg.{DenseVector, SparseVector, VectorBuilder}
import breeze.math.Semiring
import scala.reflect.ClassTag

/**
  * Created by chaomai on 03/05/2017.
  */
class LocalCoordinateVector[
    @specialized(Double, Float, Int, Long) V: ClassTag: Semiring](
    val size: Int,
    private val storage: IndexedSeq[VEntry[V]])
    extends SparseTensor[V] {

  val shape: IndexedSeq[Int] = IndexedSeq(size)
  val dimension: Int = 1

  require(shape.length == 1, s"Vector should be 1 dimension")

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
    val s1 = size
    val s2 = y.size

    require(s1 == s2,
            s"Requested inner product, "
              + s"but got vector1 with size $s1 and vector2 with size $s2")

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
    require(
      size == y.size,
      s"Requested inner product" +
        "but got vector1 with size $size and vector2 with size ${y.size}")

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
  def vals[V: ClassTag: Semiring](vs: V*): LocalCoordinateVector[V] =
    LocalCoordinateVector(vs.size,
                          vs.zipWithIndex
                            .map(p => VEntry(p._2, p._1))
                            .filter(e => e.value != 0)
                            .toIndexedSeq)

  def apply[V: ClassTag: Semiring](
      size: Int,
      vec: IndexedSeq[VEntry[V]]): LocalCoordinateVector[V] =
    new LocalCoordinateVector[V](size, vec)
}
