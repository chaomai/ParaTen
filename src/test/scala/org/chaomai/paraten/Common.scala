package org.chaomai.paraten

import org.apache.spark.{SparkConf, SparkContext}
import org.chaomai.paraten.tensor.CoordinateTensor

/**
  * Created by chaomai on 09/05/2017.
  */
class Common

object Common {
  private val conf =
    new SparkConf()
      .setAppName("Test")
      .setMaster("local[4]")
      .set("spark.executor.memory", "2g")

  private val sc = new SparkContext(conf)

  val sparkContext: SparkContext = sc

  val sizeOfDim3Tensor: IndexedSeq[Int] = IndexedSeq(5, 4, 3)

  val dim3Tensor: CoordinateTensor[Double] =
    CoordinateTensor.fromFile("src/test/resources/data/test_dim3.tensor",
                              sizeOfDim3Tensor)(sc)

  val sizeOfDim4SparseTensor: IndexedSeq[Int] = IndexedSeq(2, 2, 3, 2)

  val dim4SparseTensor: CoordinateTensor[Double] =
    CoordinateTensor.fromFile(
      "src/test/resources/data/test_dim4_sparse.tensor",
      sizeOfDim4SparseTensor)(sc)

  val sizeOfDim4DenseTensor: IndexedSeq[Int] = IndexedSeq(2, 2, 3, 2)

  val dim4DenseTensor: CoordinateTensor[Double] =
    CoordinateTensor.fromFile("src/test/resources/data/test_dim4_dense.tensor",
                              sizeOfDim4DenseTensor)(sc)

  def debugMessage(str: String): Unit =
    println("---------- %s ----------".format(str))
}
