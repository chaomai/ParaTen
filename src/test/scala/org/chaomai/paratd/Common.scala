package org.chaomai.paratd

import org.apache.spark.{SparkConf, SparkContext}
import org.chaomai.paratd.tensor.{CoordinateTensor, TensorFactory}

/**
  * Created by chaomai on 09/05/2017.
  */
class Common

object Common {
  private val conf =
    new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  private val sc = new SparkContext(conf)

  val sparkContext: SparkContext = sc

  val sizeOfDim3Tensor: IndexedSeq[Int] = IndexedSeq(5, 4, 3)

  val dim3Tensor: CoordinateTensor[Double] =
    TensorFactory.fromFile("src/main/resources/data/test_dim3.tensor",
                           sizeOfDim3Tensor)(sc)

  val sizeOfDim4SparseTensor: IndexedSeq[Int] = IndexedSeq(2, 2, 3, 2)

  val dim4SparseTensor: CoordinateTensor[Double] =
    TensorFactory.fromFile("src/main/resources/data/test_dim4.tensor",
                           sizeOfDim4SparseTensor)(sc)

  val sizeOfDim4DenseTensor: IndexedSeq[Int] = IndexedSeq(2, 2, 3, 2)

  val dim4DenseTensor: CoordinateTensor[Double] =
    TensorFactory.fromFile("src/main/resources/data/test_dim4_dense.tensor",
                           sizeOfDim4DenseTensor)(sc)

  def debugMessage(str: String): Unit =
    println("---------- %s ----------".format(str))
}
