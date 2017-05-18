package org.chaomai.paraten.vector

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import breeze.math.Semiring
import org.chaomai.paraten.support.CanUse

import scala.reflect.ClassTag

/**
  * Created by chaomai on 14/05/2017.
  */
object LocalVectorOps {
  def >*[@specialized(Double, Float, Int, Long) V: Semiring: CanUse](
      v1: BDV[V],
      v2: BDV[V]): V = {
    require(v1.size == v2.size,
            s"Requires inner product, "
              + s"but got v1.size = ${v1.size} and v2.size = ${v2.size}")

    v1 dot v2
  }

  def >*[@specialized(Double, Float, Int, Long) V: ClassTag: Semiring: CanUse](
      v1: BSV[V],
      v2: BDV[V])(implicit n: Numeric[V]): V = {
    require(v1.size == v2.size,
            s"Requires inner product, "
              + s"but got v1.size = ${v1.size} and v2.size = ${v2.size}")

    v1.toDenseVector dot v2
  }
}
