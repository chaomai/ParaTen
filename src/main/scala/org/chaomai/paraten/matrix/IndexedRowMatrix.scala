package org.chaomai.paraten.matrix

import breeze.linalg.{
  isClose,
  * => BBCOp,
  CSCMatrix => BCSCM,
  DenseMatrix => BDM,
  DenseVector => BDV,
  VectorBuilder => BVB
}
import breeze.math.Semiring
import breeze.numerics.sqrt
import breeze.stats.distributions.Rand
import breeze.storage.Zero
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chaomai.paraten.support.{CanApproximatelyEqual, CanUse}

import scala.reflect.ClassTag

/**
  * Created by chaomai on 11/05/2017.
  */
case class IndexedRow[V: CanUse](ridx: Long, rvec: BDV[V])

class IndexedRowMatrix[
    @specialized(Double, Float, Int, Long) V: ClassTag: Zero: Semiring: CanUse](
    private var nrows: Long,
    private var ncols: Int,
    private val storage: RDD[IndexedRow[V]])
    extends Matrix[V] {
  val shape: (Long, Int) = (numRows, numCols)

  def numRows: Long = {
    if (nrows <= 0) {
      nrows = storage.map(_.ridx).reduce(math.max) + 1L
    }
    nrows
  }

  def numCols: Int = {
    if (ncols <= 0) {
      ncols = storage.map(_.rvec.length).reduce(math.max) + 1
    }
    ncols
  }

  override def nnz(implicit n: Numeric[V]): Long = {
    storage
      .map(_.rvec.map(v => if (v != n.zero) 1L else 0L).reduce(_ + _))
      .reduce(_ + _)
  }

  def map[U: ClassTag: Zero: Semiring: CanUse](
      f: IndexedRow[V] => IndexedRow[U]): IndexedRowMatrix[U] =
    IndexedRowMatrix(numRows, numCols, storage.map(f))

  def mapStorage[U: ClassTag](f: IndexedRow[V] => U): RDD[U] = storage.map(f)

  def saveAsTextFile(path: String): Unit = storage.saveAsTextFile(path)

  def addRow(row: RDD[IndexedRow[V]]): IndexedRowMatrix[V] = {
    IndexedRowMatrix(numRows, numCols, storage.union(row))
  }

  /***
    * Convert to DenseMatrix.
    *
    * 1. Operation can be expensive.
    * 2. The max numRow is limited to Int.MaxValue.
    *
    * @return
    */
  def toDenseMatrix: BDM[V] = {
    toCSCMatrix.toDenseMatrix
  }

  /***
    * Convert to CSCMatrix.
    *
    * 1. Operation can be expensive.
    * 2. The max numRow is limited to Int.MaxValue.
    *
    * @return
    */
  def toCSCMatrix: BCSCM[V] = {
    val nrs = if (numRows > Int.MaxValue) Int.MaxValue else numRows.toInt

    val builder = new BCSCM.Builder[V](nrs, numCols)

    storage.collect().foreach { p =>
      val ridx = p.ridx.toInt
      val row = p.rvec

      row.foreachPair((cidx, v) => builder.add(ridx, cidx, v))
    }

    builder.result()
  }

  /***
    * Get the ridx th row.
    *
    * @param ridx row index.
    * @return     row vector.
    */
  def rowAt(ridx: Long): RDD[BDV[V]] =
    storage.filter(row => row.ridx == ridx).map(row => row.rvec)

  /***
    * Get the ridx th row.
    *
    * @param ridx row index.
    * @return     row vector.
    */
  def localRowAt(ridx: Long): BDV[V] =
    storage
      .filter(row => row.ridx == ridx)
      .map(row => row.rvec)
      .collect()
      .head

  /***
    * Get the cidx th column.
    *
    * 1. Operation can be expensive.
    * 2. The size is limited to Int.MaxValue.
    *
    * @param cidx column index.
    * @return     local column vector.
    */
  def localColAt(cidx: Int): BDV[V] = {
    require(
      numRows <= Int.MaxValue,
      s"Required column at $cidx, but the rows is bigger than ${Int.MaxValue} ")
    require(numCols > cidx,
            s"Required column at $cidx, but matrix has $numCols columns")

    val nrs = numRows.toInt

    val builder = new BVB[V](nrs)
    storage.map(row => (row.ridx.toInt, row.rvec(cidx))).collect().foreach {
      p =>
        builder.add(p._1, p._2)
    }

    builder.toDenseVector
  }

  def t: IndexedColumnMatrix[V] =
    IndexedColumnMatrix(numCols,
                        numRows,
                        storage.map(row => IndexedColumn(row.ridx, row.rvec)))

  def *(m: BDM[V]): IndexedRowMatrix[V] = {
    require(numCols == m.rows,
            s"Required matrix product, "
              + s"but the m1.numCols = $numCols and m2.numRows = ${m.rows}")

    val r = storage.map { row =>
      val ridx = row.ridx
      val rvec = row.rvec

      val vec = m(::, BBCOp) dot rvec
      IndexedRow(ridx, vec.t)
    }

    IndexedRowMatrix(numRows, m.cols, r)
  }

  def *(m: IndexedColumnMatrix[V]): IndexedRowMatrix[V] = {
    require(m.numCols <= Int.MaxValue, s"m2.numCols should <= ${Int.MaxValue}")

    require(numCols == m.numRows,
            s"Required matrix product, "
              + s"but the m1.numCols = $numCols and m2.numRows = ${m.numRows}")

    val rows = storage.map(row => (row.ridx, row.rvec))
    val cols = m.mapStorage(col => (col.cidx, col.cvec))

    val r = rows
      .cartesian(cols)
      .map { x =>
        val (rid, rvec) = (x._1._1, x._1._2)
        val (cid, cvec) = (x._2._1, x._2._2)
        (rid, (cid, rvec dot cvec))
      }
      .groupByKey()
      .map { x =>
        val (rid, vps) = (x._1, x._2)
        val vs = vps.toSeq.sortBy(_._1).map(p => p._2)
        IndexedRow(rid, BDV(vs: _*))
      }

    IndexedRowMatrix(numRows, m.numCols.toInt, r)
  }

  def norm(implicit n: Numeric[V]): BDV[Double] =
    normalizeByCol._2

  def normalizeByCol(
      implicit n: Numeric[V]): (IndexedRowMatrix[Double], BDV[Double]) =
    n match {
      case num: Fractional[V] => {
        val sqrSum = storage
          .mapPartitions { iter =>
            val sqrParSum =
              iter.foldLeft(BDV.zeros[V](numCols))((acc, idxrow) =>
                acc + (idxrow.rvec :* idxrow.rvec))
            Iterator.single(sqrParSum)
          }
          .fold(BDV.zeros[V](numCols))((v1, v2) => v1 + v2)
          .map(num.toDouble)

        val norms = sqrSum.map(sqrt(_))

        val m = IndexedRowMatrix[Double](
          numRows,
          numCols,
          storage.map(row =>
            IndexedRow(row.ridx, row.rvec.map(num.toDouble) :/ norms)))

        (m, norms)
      }
      case _ => sys.error("Operation on unsupported type.")
    }

  /***
    * Elementwise approximately equality.
    *
    * @param m          another matrix.
    * @param tol        tolerance.
    * @param n          implicit Numeric.
    * @param approxEq   implicit CanApproximatelyEqual.
    * @return           equality.
    */
  def :~==(m: IndexedRowMatrix[V], tol: Double = 1e-3)(
      implicit n: Numeric[V],
      approxEq: CanApproximatelyEqual[V]): Boolean = {
    if ((numRows != m.numRows) || (numCols != m.numCols)) false
    else {
      val rows1 = storage.map(row => (row.ridx, row.rvec))
      val rows2 = m.storage.map(row => (row.ridx, row.rvec))

      rows1
        .fullOuterJoin(rows2)
        .map { x =>
          val rvec1 = x._2._1
          val rvec2 = x._2._2

          (rvec1, rvec2) match {
            case (None, None) => sys.error("should not happen")
            case (Some(_), None) =>
              rvec1.get
                .map(v => isClose(n.toDouble(v), 0d, tol))
                .reduce(_ && _)
            case (None, Some(_)) =>
              rvec2.get
                .map(v => isClose(0d, n.toDouble(v), tol))
                .reduce(_ && _)
            case (Some(_), Some(_)) =>
              isClose(rvec1.get.map(n.toDouble),
                      rvec2.get.map(n.toDouble),
                      tol)
          }
        }
        .reduce(_ && _)
    }
  }
}

object IndexedRowMatrix {
  def zeros[V: ClassTag: Zero: Semiring: CanUse](numRows: Long, numCols: Int)(
      implicit sc: SparkContext): IndexedRowMatrix[V] =
    IndexedRowMatrix(numRows, numCols, sc.emptyRDD[IndexedRow[V]])

  def rand[V: ClassTag: Zero: Semiring: CanUse](numRows: Long,
                                                numCols: Int,
                                                rand: Rand[V] = Rand.uniform)(
      implicit sc: SparkContext): IndexedRowMatrix[V] = {
    val rows = for { ridx <- 0L until numRows } yield
      IndexedRow(ridx, BDV.rand[V](numCols, rand))

    IndexedRowMatrix(numRows, numCols, sc.parallelize(rows))
  }

  def vals[V: ClassTag: Zero: Semiring: CanUse](rows: Seq[V]*)(
      implicit sc: SparkContext): IndexedRowMatrix[V] = {
    val nrows = rows.length
    val ncols = rows.head.length

    val r = rows.zipWithIndex.map { p =>
      val row = p._1
      val ridx = p._2

      val builder = new BVB[V](ncols)

      row.zipWithIndex.foreach { x =>
        val cidx = x._2
        val v = x._1
        builder.add(cidx, v)
      }

      IndexedRow(ridx, builder.toDenseVector)
    }

    IndexedRowMatrix(nrows, ncols, sc.parallelize(r))
  }

  def apply[V: ClassTag: Zero: Semiring: CanUse](
      numRows: Long,
      numCols: Int,
      rdd: RDD[IndexedRow[V]]): IndexedRowMatrix[V] =
    new IndexedRowMatrix[V](numRows, numCols, rdd)
}
