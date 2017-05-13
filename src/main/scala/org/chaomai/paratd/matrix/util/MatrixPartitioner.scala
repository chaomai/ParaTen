package org.chaomai.paratd.matrix.util

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * Created by chaomai on 11/05/2017.
  */
object MatrixPartitioner {
  class MatrixBalancePartitioner[V](numPart: Int, rdd: RDD[(Long, BDV[V])])
      extends Partitioner {
    override def numPartitions: Int = ???

    override def getPartition(key: Any): Int = ???
  }
}
