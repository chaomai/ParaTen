package org.chaomai.paraten.tensor

import breeze.linalg.{isClose, DenseVector, SparseVector, VectorBuilder}
import breeze.math.Semiring
import breeze.numerics.sqrt
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chaomai.paraten.matrix.IndexedRowMatrix
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
    * Norm of a tensor.
    * @param n  implicit Numeric.
    * @return   norm.
    */
  def norm(implicit n: Numeric[V]): Double = {
    val fibers = fibersOnMode(0)
    val sqrSumOfNorm = fibers
      .map { p =>
        val (fi, _) = (p._1, p._2)
        fi.map(v => n.times(v, v)).reduce(n.plus)
      }
      .mapPartitions { ns =>
        Iterator.single(ns.foldLeft(n.zero)((acc, v) => n.plus(acc, v)))
      }
      .reduce(n.plus)

    sqrt(n.toDouble(sqrSumOfNorm))
  }

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
    * @return   tensor that dimension m is 1.
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

object CoordinateTensorOps {
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
