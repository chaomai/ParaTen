package org.chaomai.paratd.matrix

import breeze.linalg.operators.OpMulInner
import breeze.linalg.{
  BroadcastedColumns,
  CSCMatrix => BCSCM,
  DenseMatrix => BDM,
  DenseVector => BDV,
  SparseVector => BSV,
  Transpose => BT,
  VectorBuilder => BVB,
  * => BBroadcastingOp
}
import breeze.math.Semiring
import breeze.numerics.sqrt
import breeze.stats.distributions.Rand
import breeze.storage.Zero
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chaomai.paratd.tensor.{Coordinate, Entry, TEntry}
import org.chaomai.paratd.support.{CanUse, NotNothing}
import org.chaomai.paratd.vector.CoordinateVector

import scala.reflect.ClassTag

/**
  * Created by chaomai on 02/05/2017.
  */
class CoordinateMatrix[
    @specialized(Double, Float, Int, Long) V: ClassTag: Zero: Semiring: CanUse](
    val rows: Int,
    val cols: Int,
    private val storage: RDD[TEntry[V]])
    extends Serializable {
  val shape: IndexedSeq[Int] = IndexedSeq(rows, cols)
  val dimension: Int = 2
  val size: Int = rows * cols

  assert(shape.length == 2)

  def foreach(f: TEntry[V] => Unit): Unit = storage.foreach(f)

  def find(f: TEntry[V] => Boolean): Option[TEntry[V]] = {
    val r = storage.filter(f)

    if (r.isEmpty()) None
    else Some(r.take(1).head)
  }

  def collect: Array[TEntry[V]] = storage.collect()

  def addEntry(rdd: RDD[TEntry[V]]): CoordinateMatrix[V] =
    CoordinateMatrix(rows, cols, storage.union(rdd))

  /***
    * collect all entries and generate a DenseMatrix.
    * Operation can be expensive.
    *
    * @return DenseMatrix.
    */
  def toDenseMatrix: BDM[V] =
    toCSCMatrix.toDenseMatrix

  /***
    * collect all entries and generate a CSCMatrix.
    * Operation can be expensive.
    *
    * @return   CSCMatrix.
    */
  def toCSCMatrix: BCSCM[V] = {
    val builder = new BCSCM.Builder[V](rows, cols)

    storage.collect().foreach { e =>
      val r = e.coordinate(0)
      val c = e.coordinate(1)
      builder.add(r, c, e.value)
    }

    builder.result
  }

  def toIndexedRowMatrix: IndexedRowMatrix[V] = {
    val r = storage.groupBy(_.coordinate(0)).map { p =>
      val ridx = p._1
      val entries = p._2

      val builder = new BVB[V](cols)
      entries.foreach(e => builder.add(e.coordinate(1), e.value))
      IndexedRow(ridx, builder.toDenseVector)
    }

    IndexedRowMatrix(rows, cols, r)
  }

  /***
    * normalize columns to length one.
    *
    * @param n  implicit Numeric.
    * @return   column normalized matrix and weights of columns.
    */
  def normalizeByCol(
      implicit n: Numeric[V]): (CoordinateMatrix[Double], BDV[Double]) =
    n match {
      case num: Fractional[V] => {
        val normalization = (v: V, norm: Double) => num.toDouble(v) / norm
        val colVecs = storage.groupBy(_.coordinate(1))

        val partitionFunc = (iter: Iterator[(Int, Iterable[TEntry[V]])]) => {
          iter.map { p =>
            val idx = p._1
            val col = p._2
            val norm = sqrt(num.toDouble(col.foldLeft(num.zero)((acc, e) =>
              num.plus(acc, num.times(e.value, e.value)))))

            ((idx, norm),
             col.map(e => TEntry(e.coordinate, normalization(e.value, norm))))
          }
        }

        val norms = colVecs
          .mapPartitions(partitionFunc)
          .flatMap(e => Iterator.single(e._1))

        val entries = colVecs.mapPartitions(partitionFunc).flatMap(_._2)

        val builder = new BVB[Double](cols)
        norms.collect.foreach(p => builder.add(p._1, p._2))

        val normVec = builder.toDenseVector

        (CoordinateMatrix(rows, cols, entries), normVec)
      }
      case _ => sys.error("Operation on unsupported type.")
    }

  /***
    * matrix.t * matrix.
    *
    * @param n  implicit Numeric.
    * @return   product.
    */
  def tProd(implicit n: Numeric[V]): BDM[V] = {
    require(rows > 0 && cols > 0,
            s"Required tProd, " + s"but the size is $rows and $cols")

    val rowsVecs = storage.groupBy(_.coordinate(0))

    val partitionFunc = (iter: Iterator[(Int, Iterable[TEntry[V]])]) => {
      iter.map { p =>
        val as = (0 until cols).map { i =>
          val entry = p._2.find(_.coordinate(1) == i)
          if (entry.isDefined) entry.get.value
          else n.zero
        }

        val v = BDV(as: _*)
        v * v.t
      }
    }

    rowsVecs
      .mapPartitions(partitionFunc)
      .fold(BDM.zeros(cols, cols))((a, b) => a + b)
  }

  /***
    * matrix product.
    *
    * @param m  another matrix.
    * @param n  implicit Numeric.
    * @return   product.
    */
  def *(m: BDM[V])(implicit n: Numeric[V]): CoordinateMatrix[V] = {
    class OpMulInnerVecImpl2 extends OpMulInner.Impl2[BDV[V], BDV[V], V] {
      override def apply(v1: BDV[V], v2: BDV[V]): V = {
        require(
          v1.length == v2.length,
          s"Required dot product of two vector,"
            + s" but got ${v1.length} for vector 1 and ${v2.length} for vector 2")

        (0 until v1.length).foldLeft(n.zero)((acc, idx) =>
          n.plus(acc, n.times(v1(idx), v2(idx))))
      }
    }

    class OpMulInnerImpl2
        extends OpMulInner.Impl2[BroadcastedColumns[BDM[V], BDV[V]],
                                 BDV[V],
                                 BT[BDV[V]]] {
      override def apply(v1: BroadcastedColumns[BDM[V], BDV[V]],
                         v2: BDV[V]): BT[BDV[V]] = {
        implicit val op = new OpMulInnerVecImpl2

        val s =
          v1.toIndexedSeq.foldLeft(Seq[V]())((acc, v) => acc :+ (v dot v2))
        BDV(s: _*).t
      }
    }

    implicit val op1 = new OpMulInnerImpl2

    require(cols == m.rows,
            s"Required matrix product, "
              + s"but got this.column = $cols and m.row = ${m.rows}")

    val rowVecs = storage.groupBy(_.coordinate(0))

    val partitionFunc = (iter: Iterator[(Int, Iterable[TEntry[V]])]) => {
      iter.map { p =>
        val as = (0 until cols).map { i =>
          val entry = p._2.find(_.coordinate(1) == i)
          if (entry.isDefined) entry.get.value
          else n.zero
        }

        val v = BDV(as: _*)
        val retRowVec = (m(::, BBroadcastingOp) dot v).t

        (0 until retRowVec.length)
          .map(idx => TEntry(Coordinate(p._1, idx), retRowVec(idx)))
      }
    }

    CoordinateMatrix(rows,
                     m.cols,
                     rowVecs.mapPartitions(partitionFunc).flatMap(seq => seq))
  }

  /***
    * row at index.
    *
    * @param idx  row index.
    * @return     row vector on rdd.
    */
  def rowAt(idx: Int): CoordinateVector[V] = {
    require(rows > idx, s"Required row at $idx, but matrix has $rows rows")

    val rowVec = storage.filter(e => e.coordinate(0) == idx)
    // TEntry2VEntry need another dimension's value to indicate the index
    CoordinateVector(cols, rowVec.map(Entry.TEntry2VEntryOnDim(1, _)))
  }

  /***
    * Collect row at idx to local.
    * Operation can be expensive.
    *
    * @param idx  row index.
    * @return     row vector.
    */
  def localRowAt(idx: Int): BSV[V] = {
    require(rows > idx, s"Required row at $idx, but matrix has $rows rows")

    val builder = new BVB[V](cols)

    storage
      .filter(e => e.coordinate(0) == idx)
      .collect()
      .foreach { e =>
        builder.add(e.coordinate(1), e.value)
      }

    builder.toSparseVector
  }

  /***
    * column at index.
    *
    * @param idx  column index.
    * @return     column vector on rdd.
    */
  def colAt(idx: Int): CoordinateVector[V] = {
    require(cols > idx, s"Required row at $idx, but matrix has $cols columns")

    val colVec = storage.filter(e => e.coordinate(1) == idx)
    CoordinateVector(cols, colVec.map(Entry.TEntry2VEntryOnDim(0, _)))
  }

  /***
    * Collect column at idx to local.
    * Operation can be expensive.
    *
    * @param idx  column index.
    * @return     column vector.
    */
  def localColAt(idx: Int): BSV[V] = {
    require(cols > idx, s"Required row at $idx, but matrix has $cols columns")

    val builder = new BVB[V](rows)

    val colVec = storage
      .filter(e => e.coordinate(1) == idx)
      .collect()
      .foreach(e => builder.add(e.coordinate(0), e.value))

    builder.toSparseVector
  }

  def apply(dim: Int*): V = {
    storage
      .filter(e => e.coordinate(0) == dim(0) && e.coordinate(1) == dim(1))
      .take(1)(0)
      .value
  }

  /***
    * toString.
    * Operation can be expensive.
    *
    * @return formatted matrix.
    */
  override def toString: String = {
    def arrayOfTEntryToString(arr: Array[TEntry[V]]): String = {
      arr.foldLeft("[") { (acc, e) =>
        (acc, e) match {
          case ("[", entry) => acc + entry.toString
          case _ => acc + ", " + e.toString
        }
      } + "]"
    }

    storage
      .collect()
      .groupBy(e => e.coordinate(0))
      .foldLeft("[")((acc, p) =>
        (acc, p) match {
          case ("[", entries) => acc + arrayOfTEntryToString(entries._2)
          case _ => acc + "\n" + arrayOfTEntryToString(p._2)
      }) + "]"
  }
}

object CoordinateMatrix {

  def zeros[V: NotNothing: ClassTag: Zero: Semiring: CanUse](
      rows: Int,
      cols: Int)(implicit sc: SparkContext): CoordinateMatrix[V] = {
    CoordinateMatrix(rows, cols, sc.emptyRDD[TEntry[V]])
  }

  def rand[@specialized(Double) V: ClassTag: Zero: Semiring: CanUse](
      rows: Int,
      cols: Int,
      rand: Rand[V] = Rand.uniform)(
      implicit sc: SparkContext): CoordinateMatrix[V] = {
    val m = BDM.rand[V](rows, cols, rand)

    val entries = for {
      r <- 0 until rows
      c <- 0 until cols
    } yield TEntry(Coordinate(IndexedSeq(r, c)), m(r, c))

    CoordinateMatrix(rows, cols, sc.parallelize(entries))
  }

  def vals[V: ClassTag: Zero: Semiring: CanUse](rows: Seq[V]*)(
      implicit sc: SparkContext): CoordinateMatrix[V] = {
    val rs = rows.length
    val cs = rows.head.length

    val entries = rows.zipWithIndex.flatMap { p =>
      val row = p._1.zipWithIndex
      val ridx = p._2

      row.map { x =>
        val v = x._1
        val cidx = x._2

        TEntry(Coordinate(ridx, cidx), v)
      }
    }

    CoordinateMatrix(rs, cs, sc.parallelize(entries))
  }

  def apply[V: ClassTag: Zero: Semiring: CanUse](
      rows: Int,
      cols: Int,
      rdd: RDD[TEntry[V]]): CoordinateMatrix[V] =
    new CoordinateMatrix[V](rows, cols, rdd)
}
