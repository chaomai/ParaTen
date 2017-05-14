package org.chaomai.paraten.matrix

/**
  * Created by chaomai on 11/05/2017.
  */
trait Matrix[V] extends Serializable {
  def nnz(implicit n: Numeric[V]): Long
}
