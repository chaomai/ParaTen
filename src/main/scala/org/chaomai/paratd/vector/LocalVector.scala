package org.chaomai.paratd.vector

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import breeze.math.Semiring
import org.chaomai.paratd.support.CanUse

/**
  * Created by chaomai on 14/05/2017.
  */
object LocalVector {
  def >*[V: Semiring: CanUse](v1: BDV[V], v2: BDV[V]): V = {
    require(v1.size == v2.size,
            s"Requires inner product, "
              + s"but got v1.size = ${v1.size} and v2.size = ${v2.size}")

    v1 dot v2
  }

  def >*[V: Semiring: CanUse](v1: BSV[V], v2: BDV[V])(
      implicit n: Numeric[V]): V = {
    require(v1.size == v2.size,
            s"Requires inner product, "
              + s"but got v1.size = ${v1.size} and v2.size = ${v2.size}")

    (0 until v1.length).foldLeft(n.zero)((acc, idx) =>
      n.plus(acc, n.times(v1(idx), v2(idx))))
  }
}
