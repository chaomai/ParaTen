package org.chaomai.paratd.vector

import breeze.linalg.{
  DenseVector => BDV,
  SparseVector => BSV,
  VectorBuilder => BVB
}
import breeze.math.Semiring
import breeze.storage.Zero
import org.chaomai.paratd.support.CanUse

import scala.reflect.ClassTag

/**
  * Created by chaomai on 10/05/2017.
  */
class LocalSparseVector[V: ClassTag: CanUse](val size: Int,
                                             private val storage: BSV[V])
    extends Vector {
  def toDenseVector: BDV[V] = storage.toDenseVector

  def toSparseVector: BSV[V] = storage

  def >*(y: CoordinateVector[V]): V = {
    require(size == y.size,
            s"Requires inner product, "
              + s"but got v1.size = $size and v2.size = ${y.size}")

    ???
  }

  def >*(y: BDV[V])(implicit n: Numeric[V]): V = {
    require(size == y.size,
            s"Requires inner product, "
              + s"but got v1.size = $size and v2.size = ${y.size}")

    (0 until storage.length).foldLeft(n.zero)((acc, idx) =>
      n.plus(acc, n.times(storage(idx), y(idx))))
  }

  override def toString: String = {
    storage.mapPairs((i, v) => v.toString + " @ " + i.toString).foldLeft("[") {
      (acc, s) =>
        (acc, s) match {
          case ("[", str) => acc + str
          case _ => acc + ", " + s
        }
    } + "]"
  }
}

object LocalSparseVector {
  def vals[V: ClassTag: Zero: Semiring: CanUse](vs: V*)(
      implicit n: Numeric[V]): LocalSparseVector[V] = {
    builder(
      vs.size,
      vs.zipWithIndex.filter(p => p._1 != n.zero).map(p => (p._2, p._1)): _*)
  }

  def builder[V: ClassTag: Zero: Semiring: CanUse](
      size: Int,
      values: (Int, V)*): LocalSparseVector[V] = {
    val builder = new BVB[V](size)

    for ((i, v) <- values) {
      builder.add(i, v)
    }

    LocalSparseVector[V](size, builder.toSparseVector)
  }

  def apply[V: ClassTag: CanUse](size: Int,
                                 vec: BSV[V]): LocalSparseVector[V] =
    new LocalSparseVector[V](size, vec)
}
