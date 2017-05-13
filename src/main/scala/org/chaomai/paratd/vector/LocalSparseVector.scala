package org.chaomai.paratd.vector

import breeze.linalg.operators.OpMulInner
import breeze.linalg.{VectorBuilder, DenseVector => BDV, SparseVector => BSV}
import breeze.math.Semiring
import breeze.storage.Zero

import scala.reflect.ClassTag

/**
  * Created by chaomai on 10/05/2017.
  */
class LocalSparseVector[V: ClassTag](val size: Int,
                                     private val storage: BSV[V])
    extends Vector {
  def toDenseVector: BDV[V] = storage.toDenseVector

  def toSparseVector: BSV[V] = storage

  def >*(y: BDV[V])(implicit n: Numeric[V]): V = {
    require(size == y.size,
            s"Requires inner product, "
              + s"but got v1.size = $size and v2.size = ${y.size}")

    implicit val opMulInnerImpl2 = new OpMulInner.Impl2[BDV[V], BDV[V], V] {
      override def apply(v1: BDV[V], v2: BDV[V]): V = {
        var acc = n.zero
        for (i <- 0 until size) {
          acc = n.plus(acc, n.times(v1(i), v2(i)))
        }
        acc
      }
    }

    storage.toDenseVector dot y
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
  def vals[V: ClassTag: Zero: Semiring](vs: V*)(
      implicit n: Numeric[V]): LocalSparseVector[V] = {
    builder(
      vs.size,
      vs.zipWithIndex.filter(p => p._1 != n.zero).map(p => (p._2, p._1)): _*)
  }

  def builder[V: ClassTag: Zero: Semiring](
      size: Int,
      values: (Int, V)*): LocalSparseVector[V] = {
    val builder = new VectorBuilder[V](size)

    for ((i, v) <- values) {
      builder.add(i, v)
    }

    new LocalSparseVector[V](size, builder.toSparseVector)
  }

  def apply[V: ClassTag](size: Int, vec: BSV[V]): LocalSparseVector[V] =
    new LocalSparseVector[V](size, vec)
}
