package org.chaomai.paratd.tensor

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
  * Created by chaomai on 16/04/2017.
  */
//class TypedCoordinateTensor[Shape](sc: SparkContext,
//                                   sv: ShapeValue[Shape],
//                                   val storage: RDD[Entry]){
//    extends SparseTensor[Double] {
//  val shape: IndexedSeq[Int] = sv.shape.toIndexedSeq
//  val dimension: Int = shape.length
//  val size: Int = shape.product
//}
//
//object TypedCoordinateTensor {
//  def zeros[Shape](sc: SparkContext,
//                   sv: ShapeValue[Shape]): TypedCoordinateTensor[Shape] = {
//
//    val rdd = sc.emptyRDD[Entry]
//    new TypedCoordinateTensor[Shape](sc, sv, rdd)
//  }
//}
//
//class TypedTensorFactory[Shape](sv: ShapeValue[Shape], sc: SparkContext) {
//  def zeros: TypedCoordinateTensor[Shape] =
//    TypedCoordinateTensor.zeros(sc, sv)
//
//  def rand: TypedCoordinateTensor[Shape] = ???
//}
//
//object TypedTensorFactory {
//  def >[Shape](sv: ShapeValue[Shape])(
//      implicit sc: SparkContext): TypedTensorFactory[Shape] =
//    new TypedTensorFactory[Shape](sv, sc)
//}
