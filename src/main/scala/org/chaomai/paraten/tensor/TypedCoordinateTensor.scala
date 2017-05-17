package org.chaomai.paraten.tensor

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by chaomai on 16/04/2017.
  */
class TypedCoordinateTensor[Shape, V](sv: ShapeValue[Shape],
                                      val storage: RDD[TEntry[V]])
    extends TypedTensor[Shape] {
  val shape: IndexedSeq[Int] = sv.shape.toIndexedSeq
  val dimension: Int = shape.length
  val size: Int = shape.product
}

object TypedCoordinateTensor {}

object TypedTensorFactory {}
