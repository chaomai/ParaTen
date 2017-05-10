package org.chaomai.paratd.tensor

import breeze.linalg.{pinv, DenseMatrix, DenseVector}
import breeze.math.Semiring
import breeze.stats.distributions.Gaussian
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by chaomai on 02/05/2017.
  */
class CoordinateTensor[V: ClassTag: Semiring: Numeric](
    sv: IndexedSeq[Int],
    private val storage: RDD[TEntry[V]])
    extends SparseTensor[V] {

  val shape: IndexedSeq[Int] = sv
  val dimension: Int = sv.length
  val size: Int = sv.product

  def foreach(f: TEntry[V] => Unit): Unit = storage.foreach(f)

  def find(f: TEntry[V] => Boolean): Option[TEntry[V]] = {
    val r = storage.filter(f)

    if (r.isEmpty()) None
    else Some(r.take(1).head)
  }

  /***
    * n mode product with vector
    *
    * @param m  mode.
    * @param v  vector.
    * @return   tensor that dimension of m is 1.
    */
  def nModeProd(m: Int, v: LocalCoordinateVector[V]): CoordinateTensor[V] = {
    val fibers = fibersOnMode(m)
    val newEntries = fibers.map { p =>
      val (fi, coord) = (p._1, p._2)
      val prod = fi >* v
      TEntry(coord, prod)
    }
    CoordinateTensor(shape.updated(m, 1), newEntries)
  }

  /***
    * Get all fibers on mode-m.
    *
    * @param m  mode.
    * @return   entries of fibers.
    */
  def fibersOnMode(m: Int): RDD[(LocalCoordinateVector[V], Coordinate)] = {
    storage
      .groupBy(e => e.coordinate.updated(m, 0))
      .map { p =>
        val (coordOfFiber, entries) = (p._1, p._2)
        val fiber = LocalCoordinateVector(
          shape(m),
          entries.map(Entry.TEntry2VEntryOnDim(m, _)).toIndexedSeq)
        (fiber, coordOfFiber)
      }
  }

  def apply(dim: Int*): V =
    storage.filter(_.coordinate == IndexedSeq(dim: _*)).take(1)(0).value

  override def toString: String =
    storage.collect().foldLeft("") { (acc, e) =>
      (acc, e) match {
        case ("", entry) => acc + entry.toString
        case _ => acc + ", " + e.toString
      }
    }
}

object CoordinateTensor {

  /***
    * CP Decomposition via ALS on Spark.
    *
    * @param tensor   CoordinateTensor.
    * @param rank     rank.
    * @param maxIter  max iterations.
    * @param tol      tolerance.
    * @param tries    number of tries.
    * @return         I_n * r matrices, vector of all the eigenvalues.
    */
  def paraCP(tensor: CoordinateTensor[Double],
             rank: Int,
             maxIter: Int = 500,
             tol: Double = 1e-6,
             tries: Int = 5)(implicit sc: SparkContext)
    : (IndexedSeq[CoordinateMatrix[Double]], DenseVector[Double]) = {

    val shape = tensor.shape

    def decoupledKR(tensor: CoordinateTensor[Double],
                    factorMatrices: IndexedSeq[CoordinateMatrix[Double]],
                    dim: Int): CoordinateMatrix[Double] = {
      val initMatrix = CoordinateMatrix.zeros[Double](shape(dim), rank)
      val krProdMatrixIndices = factorMatrices.indices.filter(_ != dim)

      (0 until rank).foldLeft(initMatrix) { (accMatrix, r) =>
        val tmpTensor =
          krProdMatrixIndices.foldLeft(tensor) { (accTensor, idx) =>
            accTensor nModeProd (idx, sc
              .broadcast(factorMatrices(idx).localColAt(r))
              .value)
          }
        accMatrix.union(tmpTensor.storage.map(e =>
          TEntry(e.coordinate dimKept dim compose r, e.value)))
      }
    }

    def paraOuterPinv(factorMatrices: IndexedSeq[CoordinateMatrix[Double]],
                      dim: Int): DenseMatrix[Double] = {
      val outerProdMatrixIndices = factorMatrices.indices.filter(_ != dim)

      val prod =
        outerProdMatrixIndices.foldLeft(DenseMatrix.ones[Double](rank, rank)) {
          (acc, idx) =>
            // Hadamard product
            acc :* factorMatrices(idx).tProd
        }

      pinv(prod)
    }

    def paraMatrixProd(decopkr: CoordinateMatrix[Double],
                       m: DenseMatrix[Double]): CoordinateMatrix[Double] = {
      val broadm = sc.broadcast(m)
      decopkr * broadm.value
    }

    var optimalFactorMatrices: IndexedSeq[CoordinateMatrix[Double]] =
      shape.map(CoordinateMatrix.zeros[Double](_, rank))
    var optimalLambda = DenseVector.zeros[Double](rank)
    var reconstructedLoss: Double = 0.0
    var optimalReconstructedLoss: Double = Double.PositiveInfinity

    // tries
    for (_ <- 0 until tries) {
      var factorMatrices = shape.map(
        CoordinateMatrix
          .rand[Double](_, rank, Gaussian(mu = 0.0, sigma = 1.0)))

      var prevHead =
        CoordinateMatrix.zeros[Double](factorMatrices.head.rows, rank)
      var lambda = DenseVector.zeros[Double](rank)

      // iteration in each try
      var iter = 0
//      while (iter < maxIter && !isConverged(prevHead, factorMatrices.head)) {
      while (iter < maxIter) {
        iter += 1

        for (idx <- shape.indices) {
          val decopkr = decoupledKR(tensor, factorMatrices, idx)
          val pinv = paraOuterPinv(factorMatrices, idx)
          val fm = paraMatrixProd(decopkr, pinv)

          val (m, l) = fm.normalizeByCol

          factorMatrices = factorMatrices.updated(idx, m)
          lambda = l
        }
      }

      // set current best
      optimalFactorMatrices = factorMatrices
      optimalLambda = lambda
    }

    (optimalFactorMatrices, optimalLambda)
  }

  def apply[V: ClassTag: Semiring: Numeric](
      sv: IndexedSeq[Int],
      rdd: RDD[TEntry[V]]): CoordinateTensor[V] = {
    new CoordinateTensor[V](sv, rdd)
  }
}

object TensorFactory {
  def fromFile(path: String, sv: IndexedSeq[Int])(
      implicit sc: SparkContext): CoordinateTensor[Double] = {
    val dim = sv.length
    val entries = sc
      .textFile(path)
      .map { l =>
        val arr = l.split(" ").map(_.trim)
        TEntry(Coordinate(arr.take(dim).map(_.toInt).toIndexedSeq),
               arr(dim).toDouble)
      }

    CoordinateTensor(sv, entries)
  }
}
