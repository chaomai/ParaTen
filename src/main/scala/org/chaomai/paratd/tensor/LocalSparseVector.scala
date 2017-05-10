package org.chaomai.paratd.tensor

import breeze.linalg.operators.OpMulInner
import breeze.linalg.{DenseVector, SparseVector}
import breeze.storage.Zero

import scala.reflect.ClassTag

/**
  * Created by chaomai on 10/05/2017.
  */
class LocalSparseVector[V: ClassTag](
    val size: Int,
    private val storage: SparseVector[V])
    extends SparseTensor[V] {
  val shape: IndexedSeq[Int] = IndexedSeq(size)
  val dimension: Int = 1

  def toDenseVector: DenseVector[V] = storage.toDenseVector

  def toSparseVector: SparseVector[V] = storage

  def >*(y: LocalSparseVector[V]): V = {
    implicit val canOpMulInnerImpl2 =
      new OpMulInner.Impl2[SparseVector[V], SparseVector[V], V] {
        override def apply(v1: SparseVector[V], v2: SparseVector[V]): V = ???
      }

    storage dot y.storage
    ???
  }
}

object LocalSparseVector {
  def vals[V:ClassTag:Zero](vs: V*): LocalSparseVector[V] =
    LocalSparseVector(vs.size, SparseVector(vs: _*))

  def apply[V:ClassTag](size: Int,
                         vec: SparseVector[V]): LocalSparseVector[V] =
    new LocalSparseVector[V](size, vec)
}
