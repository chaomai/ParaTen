package org.chaomai.paraten.tensor

import breeze.linalg.{isClose, pinv, DenseMatrix, DenseVector}
import breeze.stats.distributions.Gaussian
import org.apache.spark.SparkContext
import org.chaomai.paraten.matrix.{CoordinateMatrix, IndexedRowMatrix}

/**
  * Created by chaomai on 16/05/2017.
  */
class CPALS(private var rank: Int = 10,
            private var maxIter: Int = 500,
            private var tol: Double = 1e-3,
            private var tries: Int = 3) {
  def setRank(r: Int): this.type = {
    require(r > 0, s"Rank of the tenor must be positive but got $r")
    rank = r
    this
  }

  def setMaxIter(mi: Int): this.type = {
    require(mi >= 0, s"Number of iterations must be nonnegative but got $mi")
    maxIter = mi
    this
  }

  def setTol(t: Double): this.type = {
    require(t >= 0, s"Tolerance must be nonnegative but got $t")
    tol = t
    this
  }

  def setTries(t: Int): this.type = {
    require(t >= 0, s"Number of tries must be nonnegative but got $t")
    tries = t
    this
  }

  private def naiveKR(
      tensor: CoordinateTensor[Double],
      facMats: IndexedSeq[IndexedRowMatrix[Double]],
      dim: Int)(implicit sc: SparkContext): IndexedRowMatrix[Double] = {
    val shape = tensor.shape
    val rank = facMats.head.numCols

    val initfmat = CoordinateMatrix.zeros[Double](shape(dim), rank)
    val krProdMatIdxs = facMats.indices.filter(_ != dim)

    val fmat = (0 until rank).foldLeft(initfmat) { (accmat, r) =>
      val tmpten = krProdMatIdxs.foldLeft(tensor) { (accten, idx) =>
        val breadCol = sc.broadcast(facMats(idx).localColAt(r))
        val prod = accten nModeProd (idx, breadCol)
        breadCol.unpersist()
        prod
      }
      accmat.addEntry(tmpten.mapStorage(e =>
        TEntry(e.coordinate dimKept dim appendDim r, e.value)))
    }

    fmat.toIndexedRowMatrix
  }

  private def paraOuterPinv(
      facMats: IndexedSeq[IndexedRowMatrix[Double]],
      dim: Int,
      rank: Int)(implicit sc: SparkContext): DenseMatrix[Double] = {
    val outerProdMatIdxs = facMats.indices.filter(_ != dim)
    val rowsOfFacMats = outerProdMatIdxs
      .map(idx => facMats(idx).mapStorage(row => (idx, row)))

    val rows = sc.union(rowsOfFacMats)

    val prodByKey = rows.aggregateByKey(DenseMatrix.zeros[Double](rank, rank))(
      (acc, row) => acc + row.rvec * row.rvec.t,
      (m1, m2) => m1 + m2)

    val prod =
      prodByKey.map(_._2).reduce(_ :* _)

    pinv(prod)
  }

  private def paraMatrixProd(decopkr: IndexedRowMatrix[Double],
                             m: DenseMatrix[Double])(
      implicit sc: SparkContext): IndexedRowMatrix[Double] = {
    val broadm = sc.broadcast(m)
    val prod = decopkr * broadm
    broadm.unpersist()
    prod
  }

  /***
    * CP Decomposition via ALS on Spark.
    *
    * @param tensor   CoordinateTensor.
    * @param sc       SparkContext.
    * @return         I_n * r matrices, vector of all the eigenvalues.
    */
  def run(tensor: CoordinateTensor[Double])(implicit sc: SparkContext)
    : ((IndexedSeq[IndexedRowMatrix[Double]], DenseVector[Double])) = {

    val shape = tensor.shape

    var optimalFacMats: IndexedSeq[IndexedRowMatrix[Double]] =
      shape.map(IndexedRowMatrix.zeros[Double](_, rank))
    var optimalLambda = DenseVector.zeros[Double](rank)
    var reconsLoss: Double = 0.0
    var optimalReconsLoss: Double = Double.PositiveInfinity

    var prevLoss = Double.PositiveInfinity

    var ntries = 0
    while ((ntries < tries) && !isClose(reconsLoss, prevLoss, tol)) {
      prevLoss = reconsLoss

      ntries += 1

      var facMats = shape.map(
        IndexedRowMatrix
          .rand[Double](_, rank, Gaussian(mu = 0.0, sigma = 0.5)))
      var lambda = DenseVector.zeros[Double](rank)

      var prevHead = IndexedRowMatrix.zeros[Double](shape(0), rank)

      var iter = 0
      while ((iter < maxIter) && !prevHead.:~==(facMats.head, tol)) {
        iter += 1

        for (idx <- shape.indices) {
          val decopkr = naiveKR(tensor, facMats, idx)
          val pinv = paraOuterPinv(facMats, idx, rank)
          val fm = paraMatrixProd(decopkr, pinv)

          val (m, l) = fm.normalizeByCol

          facMats = facMats.updated(idx, m)
          lambda = l
        }

        println(s"Iteration: $iter")
      }

      // get loss
      val reconsTen =
        CoordinateTensor.fromFacMats(shape, rank, facMats, lambda)
      reconsLoss = (tensor :- reconsTen).norm

      println(s"Total Iteration: $iter, loss: $reconsLoss")

      // set current best
      if (reconsLoss < optimalReconsLoss) {
        optimalFacMats = facMats
        optimalLambda = lambda
        optimalReconsLoss = reconsLoss
      }
    }

    (optimalFacMats, optimalLambda)
  }
}
