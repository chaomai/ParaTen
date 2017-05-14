package org.chaomai.paratd.tensor

import breeze.linalg.{
  pinv,
  DenseMatrix,
  DenseVector,
  SparseVector,
  VectorBuilder
}
import breeze.math.Semiring
import breeze.stats.distributions.Gaussian
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chaomai.paratd.matrix.{CoordinateMatrix, IndexedRowMatrix}
import org.chaomai.paratd.support.CanUse
import org.chaomai.paratd.vector.LocalVector

import scala.reflect.ClassTag

/**
  * Created by chaomai on 02/05/2017.
  */
class CoordinateTensor[V: ClassTag: Semiring: CanUse](
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

  def map[U: ClassTag: Semiring: CanUse](
      f: TEntry[V] => TEntry[U]): CoordinateTensor[U] =
    CoordinateTensor(shape, storage.map(f))

  def mapStorage[U: ClassTag](f: TEntry[V] => U): RDD[U] =
    storage.map(f)

  def collect: Array[TEntry[V]] = storage.collect()

  def ~=(t: CoordinateTensor[V], tol: Double = 1e-3): (Double, Boolean) = {
    val t1 = storage.map(e => (e.coordinate, e.value))
    val t2 = t.storage.map(e => (e.coordinate, e.value))

//    t1.fullOuterJoin(t2).map { x =>
//      val coord = x._1
//      val e1 = x._2._1
//      val e2 = x._2._2
//
//      (e1, e2) match {
//        case (None, None) => true
//        case (None, Some(v))=>
//      }
//    }

    ???
  }

  def <*(v: DenseVector[V]): CoordinateTensor[V] = ???

  /***
    * n mode product with vector
    *
    * @param m  mode.
    * @param v  vector.
    * @return   tensor that dimension of m is 1.
    */
  def nModeProd(m: Int, v: DenseVector[V])(
      implicit n: Numeric[V]): CoordinateTensor[V] = {
    val fibers = fibersOnMode(m)
    val newEntries = fibers.map { p =>
      val (fi, coord) = (p._1, p._2)
      val prod = LocalVector.>*(fi, v)
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
  def fibersOnMode(m: Int): RDD[(SparseVector[V], Coordinate)] = {
    storage
      .groupBy(e => e.coordinate.updated(m, 0))
      .map { p =>
        val (coordOfFiber, entries) = (p._1, p._2)
        val builder = new VectorBuilder[V](shape(m))

        entries.foreach(e => builder.add(e.coordinate(m), e.value))

        val sv = builder.toSparseVector()
        (sv, coordOfFiber)
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
      facMats: IndexedSeq[IndexedRowMatrix[Double]],
      dim: Int)(implicit sc: SparkContext): IndexedRowMatrix[Double] = {
    val shape = tensor.shape
    val rank = facMats.head.numCols

    val initfmat = CoordinateMatrix.zeros[Double](shape(dim), rank)
    val krProdMatIdx = facMats.indices.filter(_ != dim)

    val fmat = (0 until rank).foldLeft(initfmat) { (accmat, r) =>
      val tmpten = krProdMatIdx.foldLeft(tensor) { (accten, idx) =>
        accten nModeProd (idx, sc.broadcast(facMats(idx).localColAt(r)).value)
      }
      accmat.addEntry(tmpten.storage.map(e =>
        TEntry(e.coordinate dimKept dim compose r, e.value)))
    }

    fmat.toIndexedRowMatrix
  }

  private def paraOuterPinv(facMats: IndexedSeq[IndexedRowMatrix[Double]],
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

  private def paraMatrixProd(decopkr: IndexedRowMatrix[Double],
                             m: DenseMatrix[Double])(
      implicit sc: SparkContext): IndexedRowMatrix[Double] = {
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
    : (IndexedSeq[IndexedRowMatrix[Double]], DenseVector[Double]) = {

    val shape = tensor.shape

    var optimalFacMats: IndexedSeq[IndexedRowMatrix[Double]] =
      shape.map(IndexedRowMatrix.zeros[Double](_, rank))
    var optimalLambda = DenseVector.zeros[Double](rank)
    var reconsLoss: Double = 0.0
    var optimalReconsLoss: Double = Double.PositiveInfinity

    for (_ <- 0 until tries) {
      var factorMats = shape.map(
        IndexedRowMatrix
          .rand[Double](_, rank, Gaussian(mu = 0.0, sigma = 1.0)))

      var prevHead = IndexedRowMatrix.zeros[Double](shape(0), rank)

      var lambda = DenseVector.zeros[Double](rank)

      var iter = 0
      while ((iter < maxIter) || (prevHead ~= factorMats.head)) {
        iter += 1

        for (idx <- shape.indices) {
          val decopkr = decoupledKR(tensor, factorMats, idx)
          val pinv = paraOuterPinv(factorMats, idx)
          val fm = paraMatrixProd(decopkr, pinv)

          val (m, l) = fm.normalizeByCol

          factorMats = factorMats.updated(idx, m)
          lambda = l
        }
      }

      // set current best
      val p = tensor ~= fromFacMats(shape, rank, factorMats, lambda)
      reconsLoss = p._1
      val isClose = p._2

      if (reconsLoss < optimalReconsLoss) {
        optimalFacMats = factorMats
        optimalLambda = lambda
        optimalReconsLoss = reconsLoss
      }
    }

    (optimalFacMats, optimalLambda)
  }

  def zero[V: ClassTag: Semiring: CanUse](sv: IndexedSeq[Int])(
      implicit sc: SparkContext): CoordinateTensor[V] = {
    CoordinateTensor(sv, sc.emptyRDD[TEntry[V]])
  }

  def apply[V: ClassTag: Semiring: CanUse](
      sv: IndexedSeq[Int],
      rdd: RDD[TEntry[V]]): CoordinateTensor[V] = {
    new CoordinateTensor[V](sv, rdd)
  }

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

  def fromFacMats(shape: IndexedSeq[Int],
                  rank: Int,
                  mats: IndexedSeq[IndexedRowMatrix[Double]],
                  lambda: DenseVector[Double])(
      implicit sc: SparkContext): CoordinateTensor[Double] = {
    val tensor = CoordinateTensor.zero[Double](shape)

    for (r <- 0 until rank) {

      var accTen = CoordinateTensor.zero[Double](shape)
      for (m <- mats) {
        accTen <* m.localColAt(r)
      }
    }

    ???
  }
}
