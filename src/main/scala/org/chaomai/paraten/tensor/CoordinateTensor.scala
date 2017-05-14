package org.chaomai.paraten.tensor

import breeze.linalg.{
  isClose,
  norm,
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
import org.chaomai.paraten.matrix.{CoordinateMatrix, IndexedRowMatrix}
import org.chaomai.paraten.support.{CanApproximatelyEqual, CanUse}
import org.chaomai.paraten.vector.LocalVectorOps

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

  def reduceStorage(f: (TEntry[V], TEntry[V]) => TEntry[V]): TEntry[V] =
    storage.reduce(f)

  def collect: Array[TEntry[V]] = storage.collect()

  /***
    * Elementwise approximately equality.
    *
    * @param t          another tensor.
    * @param tol        tolerance.
    * @param n          implicit Numeric.
    * @param approxEq   implicit CanApproximatelyEqual.
    * @return           equality.
    */
  def :~==(t: CoordinateTensor[V], tol: Double = 1e-3)(
      implicit n: Numeric[V],
      approxEq: CanApproximatelyEqual[V]): Boolean = {
    if (shape != t.shape) false
    else {
      val t1 = storage.map(e => (e.coordinate, e.value))
      val t2 = t.storage.map(e => (e.coordinate, e.value))

      t1.fullOuterJoin(t2)
        .map { x =>
          val e1 = x._2._1
          val e2 = x._2._2

          (e1, e2) match {
            case (None, None) => sys.error("should not happen")
            case (Some(_), None) => isClose(n.toDouble(e1.get), 0d, tol)
            case (None, Some(_)) => isClose(0d, n.toDouble(e2.get), tol)
            case (Some(_), Some(_)) =>
              isClose(n.toDouble(e1.get), n.toDouble(e2.get), tol)
          }
        }
        .reduce(_ && _)
    }
  }

  /***
    * Elementwise addition.
    *
    * @param t  another tensor.
    * @param n  implicit Numeric.
    * @return   addition result.
    */
  def :+(t: CoordinateTensor[V])(implicit n: Numeric[V]): CoordinateTensor[V] =
    CoordinateTensorOps.elementwiseOp(this, t)(n.plus)(v => v)(v => v)

  /***
    * Scalar addition.
    *
    * @param s  another tensor.
    * @param n  implicit Numeric.
    * @return   plus result.
    */
  def :+(s: V)(implicit n: Numeric[V]): CoordinateTensor[V] =
    map(_.map(n.plus(_, s)))

  /***
    * Elementwise subtraction.
    *
    * @param t  another tensor.
    * @param n  implicit Numeric.
    * @return   minus result.
    */
  def :-(t: CoordinateTensor[V])(implicit n: Numeric[V]): CoordinateTensor[V] =
    CoordinateTensorOps.elementwiseOp(this, t)(n.minus)(v => v)(v => v)

  /***
    * Scalar subtraction.
    *
    * @param s  another tensor.
    * @param n  implicit Numeric.
    * @return   minus result.
    */
  def :-(s: V)(implicit n: Numeric[V]): CoordinateTensor[V] =
    map(_.map(n.minus(_, s)))

  /***
    * Elementwise production.
    *
    * @param t  another tensor.
    * @param n  implicit Numeric.
    * @return   times result.
    */
  def :*(t: CoordinateTensor[V])(implicit n: Numeric[V]): CoordinateTensor[V] =
    CoordinateTensorOps.elementwiseOp(this, t)(n.times)(v => v)(v => v)

  /***
    * Scalar production.
    *
    * @param s  another tensor.
    * @param n  implicit Numeric.
    * @return   times result.
    */
  def :*(s: V)(implicit n: Numeric[V]): CoordinateTensor[V] =
    map(_.map(n.times(_, s)))

  /***
    * Outer product with vector.
    *
    * @param vec  vector.
    * @param n  implicit Numeric.
    * @return   outer product result.
    */
  def <*(vec: DenseVector[V])(implicit n: Numeric[V]): CoordinateTensor[V] = {
    val t = storage.flatMap { e =>
      val coord = e.coordinate
      val ev = e.value

      vec.mapPairs { (idx, v) =>
        TEntry(coord appendDim idx, n.times(v, ev))
      }.toArray
    }

    CoordinateTensor(shape :+ vec.length, t)
  }

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
      val prod = LocalVectorOps.>*(fi, v)
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
    storage.collect().foldLeft("[") { (acc, e) =>
      (acc, e) match {
        case ("[", entry) => acc + entry.toString
        case _ => acc + ", " + e.toString
      }
    } + "]"
}

object CoordinateTensor {
  private def decoupledKR(
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
        val prod = accten nModeProd (idx, breadCol.value)
        breadCol.unpersist()
        prod
      }
      accmat.addEntry(tmpten.storage.map(e =>
        TEntry(e.coordinate dimKept dim appendDim r, e.value)))
    }

    fmat.toIndexedRowMatrix
  }

  private def paraOuterPinv(facMats: IndexedSeq[IndexedRowMatrix[Double]],
                            dim: Int): DenseMatrix[Double] = {
    val rank = facMats.head.numCols
    val outerProdMatIdxs = facMats.indices.filter(_ != dim)

    val prod =
      outerProdMatIdxs.foldLeft(DenseMatrix.ones[Double](rank, rank)) {
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
    val prod = decopkr * broadm.value
    broadm.unpersist()
    prod
  }

  private def loss(t: CoordinateTensor[Double]): Double = {
    val fibers = t.fibersOnMode(0)
    val norms = fibers
      .map { p =>
        val (fi, _) = (p._1, p._2)
        norm(fi)
      }
      .collect()
      .toSeq

    norm(DenseVector(norms: _*))
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
             tol: Double = 1e-3,
             tries: Int = 3)(implicit sc: SparkContext)
    : (IndexedSeq[IndexedRowMatrix[Double]], DenseVector[Double]) = {

    val shape = tensor.shape

    var optimalFacMats: IndexedSeq[IndexedRowMatrix[Double]] =
      shape.map(IndexedRowMatrix.zeros[Double](_, rank))
    var optimalLambda = DenseVector.zeros[Double](rank)
    var reconsLoss: Double = 0.0
    var optimalReconsLoss: Double = Double.PositiveInfinity

    for (_ <- 0 until tries) {
      var facMats = shape.map(
        IndexedRowMatrix
          .rand[Double](_, rank, Gaussian(mu = 0.0, sigma = 1.0)))

      var prevHead = IndexedRowMatrix.zeros[Double](shape(0), rank)

      var lambda = DenseVector.zeros[Double](rank)

      var iter = 0
      while ((iter < maxIter) && !prevHead.:~==(facMats.head, tol)) {
        iter += 1

        for (idx <- shape.indices) {
          val decopkr = decoupledKR(tensor, facMats, idx)
          val pinv = paraOuterPinv(facMats, idx)
          val fm = paraMatrixProd(decopkr, pinv)

          val (m, l) = fm.normalizeByCol

          facMats = facMats.updated(idx, m)
          lambda = l
        }

        println("iter: %d".format(iter))
      }

      // get loss
      val reconsTen =
        CoordinateTensor.fromFacMats(shape, rank, facMats, lambda)
      reconsLoss = loss(tensor :- reconsTen)

      println("iter: %d, loss: %f".format(iter, reconsLoss))
      facMats.foreach(e => println(e.toDenseMatrix))
      println(lambda)
      println(reconsTen)

      // set current best
      if (reconsLoss < optimalReconsLoss) {
        optimalFacMats = facMats
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

  def vals[V: ClassTag: Semiring: CanUse](sv: IndexedSeq[Int], vs: TEntry[V]*)(
      implicit sc: SparkContext): CoordinateTensor[V] =
    CoordinateTensor[V](sv, sc.parallelize(vs))

  def apply[V: ClassTag: Semiring: CanUse](
      sv: IndexedSeq[Int],
      rdd: RDD[TEntry[V]]): CoordinateTensor[V] = {
    new CoordinateTensor[V](sv, rdd)
  }

  def fromDenseVector[V: ClassTag: Semiring: CanUse](
      sv: Int,
      v: DenseVector[V])(implicit sc: SparkContext): CoordinateTensor[V] = {
    val entries = v
      .mapPairs((idx, v) => TEntry(idx, v))
      .toArray
      .toSeq

    CoordinateTensor(IndexedSeq(sv), sc.parallelize(entries))
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
    (0 until rank).foldLeft(CoordinateTensor.zero[Double](shape)) {
      (accten, r) =>
        val initTen =
          CoordinateTensor.fromDenseVector(shape(0), mats.head.localColAt(r))

        val facMatsIdxs = mats.indices.tail

        val tmpten = facMatsIdxs.foldLeft(initTen) { (acc, idx) =>
          val broadRow = sc.broadcast(mats(idx).localColAt(r))
          val prod = acc <* broadRow.value
          broadRow.unpersist()
          prod
        }

        accten :+ (tmpten :* lambda(r))
    }
  }
}

private object CoordinateTensorOps {
  def elementwiseOp[V: ClassTag: Semiring: CanUse,
                    U: ClassTag: Semiring: CanUse,
                    W: ClassTag: Semiring: CanUse](t1: CoordinateTensor[V],
                                                   t2: CoordinateTensor[U])(
      f1: (V, U) => W)(f2: V => W)(f3: U => W): CoordinateTensor[W] = {
    require(t1.shape == t2.shape,
            s"Required elementwise operation, " +
              s"but get t1.shape = ${t1.shape} and t2.shape = ${t2.shape}")

    val shape = t1.shape

    val tp1 = t1.mapStorage(e => (e.coordinate, e.value))
    val tp2 = t2.mapStorage(e => (e.coordinate, e.value))

    val tn = tp1
      .fullOuterJoin(tp2)
      .map { x =>
        val coord = x._1
        val e1 = x._2._1
        val e2 = x._2._2

        (e1, e2) match {
          case (None, None) => sys.error("should not happen")
          case (Some(_), None) => TEntry(coord, f2(e1.get))
          case (None, Some(_)) => TEntry(coord, f3(e2.get))
          case (Some(_), Some(_)) =>
            TEntry(coord, f1(e1.get, e2.get))
        }
      }

    CoordinateTensor(shape, tn)
  }
}
