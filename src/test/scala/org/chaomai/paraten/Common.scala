package org.chaomai.paraten

import org.apache.spark.{SparkConf, SparkContext}
import org.chaomai.paraten.tensor.CoordinateTensor

/**
  * Created by chaomai on 09/05/2017.
  */
object Common {
  private val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
    .set("spark.executor.memory", "2g")

  val sc: SparkContext = new SparkContext(conf)

  val dim3TensorSize: IndexedSeq[Int] = IndexedSeq(5, 4, 3)
  val dim3TensorPath = "src/test/resources/data/test_dim3.tensor"
  val dim3Tensor: CoordinateTensor[Double] =
    CoordinateTensor.fromFile(dim3TensorPath, dim3TensorSize)(sc)

  val dim4SparseTensorSize: IndexedSeq[Int] = IndexedSeq(2, 2, 3, 2)
  val dim4SparseTensorPath = "src/test/resources/data/test_dim4_sparse.tensor"
  val dim4SparseTensor: CoordinateTensor[Double] =
    CoordinateTensor.fromFile(dim4SparseTensorPath, dim4SparseTensorSize)(sc)

  val dim4DenseTensorSize: IndexedSeq[Int] = IndexedSeq(2, 2, 3, 2)
  val dim4DenseTensorPath = "src/test/resources/data/test_dim4_dense.tensor"
  val dim4DenseTensor: CoordinateTensor[Double] =
    CoordinateTensor.fromFile(dim4DenseTensorPath, dim4DenseTensorSize)(sc)

  def debugMessage(str: String): Unit =
    println("---------- %s ----------".format(str))
}
