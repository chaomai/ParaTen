package org.chaomai.paratd.tensor

import breeze.linalg.{pinv, DenseMatrix, DenseVector}
import breeze.math.Semiring
import breeze.stats.distributions.Gaussian
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.chaomai.paratd.matrix.IndexedRowMatrix
import org.chaomai.paratd.vector.{LocalCoordinateVector, LocalSparseVector}

import scala.reflect.ClassTag

/**
  * Created by chaomai on 02/05/2017.
  */
class CoordinateTensor[V: ClassTag: Semiring: Numeric](
    sv: IndexedSeq[Int],
    private val storage: RDD[TEntry[V]])
    extends Serializable {
  val shape: IndexedSeq[Int] = sv
  val dimension: Int = sv.length
  val size: Int = sv.product

  def foreach(f: TEntry[V] => Unit): Unit = storage.foreach(f)

  def find(f: TEntry[V] => Boolean): Option[TEntry[V]] = {
    val r = storage.filter(f)

    if (r.isEmpty()) None
    else Some(r.take(1).head)
  }

  def map[U: ClassTag: Semiring](f: TEntry[V] => TEntry[U])(
      implicit n: Numeric[U]): CoordinateTensor[U] =
    CoordinateTensor(shape, storage.map(f))

  def mapStorage[U: ClassTag](f: TEntry[V] => U): RDD[U] =
    storage.map(f)

  def collect: Array[TEntry[V]] = storage.collect()

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

  def nModeProd1(m: Int, v: DenseVector[V]): CoordinateTensor[V] = {
    val fibers = fibersOnMode1(m)
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

  def fibersOnMode1(m: Int): RDD[(LocalSparseVector[V], Coordinate)] = {
    storage
      .groupBy(e => e.coordinate.updated(m, 0))
      .map { p =>
        val (coordOfFiber, entries) = (p._1, p._2)

        val values = entries
          .map(Entry.TEntry2VEntryOnDim(m, _))
          .map(e => (e.coordinate, e.value))
          .toSeq

        val lsv = LocalSparseVector.builder(shape(m), values: _*)
        (lsv, coordOfFiber)
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
  private def decoupledKR(
      tensor: CoordinateTensor[Double],
      factorMatrices: IndexedSeq[CoordinateMatrix[Double]],
      dim: Int)(implicit sc: SparkContext): CoordinateMatrix[Double] = {
    val shape = tensor.shape
    val rank = factorMatrices.head.cols
    val initMatrix = CoordinateMatrix.zeros[Double](shape(dim), rank)
    val krProdMatrixIndices = factorMatrices.indices.filter(_ != dim)

    (0 until rank).foldLeft(initMatrix) { (accMatrix, r) =>
      val tmpTensor =
        krProdMatrixIndices.foldLeft(tensor) { (accTensor, idx) =>
          accTensor nModeProd (idx, sc
            .broadcast(factorMatrices(idx).localColAt(r))
            .value)
        }
      accMatrix.addEntry(tmpTensor.storage.map(e =>
        TEntry(e.coordinate dimKept dim compose r, e.value)))
    }
  }

  private def paraOuterPinv(
      factorMatrices: IndexedSeq[CoordinateMatrix[Double]],
      dim: Int): DenseMatrix[Double] = {
    val rank = factorMatrices.head.cols
    val outerProdMatrixIndices = factorMatrices.indices.filter(_ != dim)

    val prod =
      outerProdMatrixIndices.foldLeft(DenseMatrix.ones[Double](rank, rank)) {
        (acc, idx) =>
          // Hadamard product
          acc :* factorMatrices(idx).tProd
      }

    pinv(prod)
  }

  private def paraMatrixProd(decopkr: CoordinateMatrix[Double],
                             m: DenseMatrix[Double])(
      implicit sc: SparkContext): CoordinateMatrix[Double] = {
    val broadm = sc.broadcast(m)
    decopkr * broadm.value
  }

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

  private def decoupledKR1(
      tensor: CoordinateTensor[Double],
      facMats: IndexedSeq[IndexedRowMatrix[Double]],
      dim: Int)(implicit sc: SparkContext): IndexedRowMatrix[Double] = {
    val shape = tensor.shape
    val rank = facMats.head.numCols

    val initfmat = CoordinateMatrix.zeros[Double](shape(dim), rank)
    val krProdMatIdx = facMats.indices.filter(_ != dim)

    val fmat = (0 until rank).foldLeft(initfmat) { (accmat, r) =>
      val tmpten = krProdMatIdx.foldLeft(tensor) { (accten, idx) =>
        accten nModeProd1 (idx, sc.broadcast(facMats(idx).localColAt(r)).value)
      }
      accmat.addEntry(tmpten.storage.map(e =>
        TEntry(e.coordinate dimKept dim compose r, e.value)))
    }

    fmat.toIndexedRowMatrix
  }

  private def paraOuterPinv1(facMats: IndexedSeq[IndexedRowMatrix[Double]],
                             dim: Int): DenseMatrix[Double] = {
    val rank = facMats.head.numCols
    val outerProdMatIdx = facMats.indices.filter(_ != dim)

    val prod = outerProdMatIdx.foldLeft(DenseMatrix.ones[Double](rank, rank)) {
      (acc, idx) =>
        val fm = facMats(idx)
        acc :* (fm.t * fm)
    }

    pinv(prod)
  }

  private def paraMatrixProd1(decopkr: IndexedRowMatrix[Double],
                              m: DenseMatrix[Double])(
      implicit sc: SparkContext): IndexedRowMatrix[Double] = {
    val broadm = sc.broadcast(m)
    decopkr * broadm.value
  }

  def paraCP1(tensor: CoordinateTensor[Double],
              rank: Int,
              maxIter: Int = 500,
              tol: Double = 1e-6,
              tries: Int = 5)(implicit sc: SparkContext)
    : (IndexedSeq[IndexedRowMatrix[Double]], DenseVector[Double]) = {

    val shape = tensor.shape

    var optimalFactorMats: IndexedSeq[IndexedRowMatrix[Double]] =
      shape.map(IndexedRowMatrix.zeros[Double](_, rank))
    var optimalLambda = DenseVector.zeros[Double](rank)
    var reconstructedLoss: Double = 0.0
    var optimalReconstructedLoss: Double = Double.PositiveInfinity

    for (_ <- 0 until tries) {
      var factorMats = shape.map(
        IndexedRowMatrix
          .rand[Double](_, rank, Gaussian(mu = 0.0, sigma = 1.0)))

      var prevHead = IndexedRowMatrix.zeros[Double](shape(0), rank)

      var lambda = DenseVector.zeros[Double](rank)

      for (iter <- 0 until maxIter) {
        for (idx <- shape.indices) {
          val decopkr = decoupledKR1(tensor, factorMats, idx)
          val pinv = paraOuterPinv1(factorMats, idx)
          val fm = paraMatrixProd1(decopkr, pinv)

          val (m, l) = fm.normalizeByCol

          factorMats = factorMats.updated(idx, m)
          lambda = l
        }
      }
    }

    ???
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
