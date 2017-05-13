package org.chaomai.paratd.matrix

import breeze.linalg.support.LiteralRow
import breeze.linalg.{
  CSCMatrix => BCSCM,
  DenseMatrix => BDM,
  DenseVector => BDV,
  VectorBuilder => BVB
}
import breeze.math.Semiring
import breeze.stats.distributions.Rand
import breeze.storage.Zero
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by chaomai on 11/05/2017.
  */
case class IndexedColumn[V](cidx: Long, cvec: BDV[V])

class IndexedColumnMatrix[
    @specialized(Double, Float, Int, Long) V: ClassTag: Zero: Semiring](
    private var nrows: Int,
    private var ncols: Long,
    private val storage: RDD[IndexedColumn[V]])
    extends Matrix[V] {
  val shape: (Int, Long) = (numRows, numCols)

  def numRows: Int = {
    if (nrows <= 0) {
      nrows = storage.map(_.cvec.length).reduce(math.max) + 1
    }
    nrows
  }

  def numCols: Long = {
    if (ncols <= 0) {
      ncols = storage.map(_.cidx).reduce(math.max) + 1L
    }
    ncols
  }

  override def nnz(implicit n: Numeric[V]): Long = {
    storage
      .map(_.cvec.map(v => if (v != n.zero) 1L else 0L).reduce(_ + _))
      .reduce(_ + _)
  }

  def mapStorage[U: ClassTag](f: IndexedColumn[V] => U): RDD[U] =
    storage.map(f)

  def addColumn(col: RDD[IndexedColumn[V]]): IndexedColumnMatrix[V] = {
    IndexedColumnMatrix(numRows, numCols, storage.union(col))
  }

  def toDenseMatrix: BDM[V] = toCSCMatrix.toDenseMatrix

  def toCSCMatrix: BCSCM[V] = {
    val ncs = if (numCols >= Int.MaxValue) Int.MaxValue else numCols.toInt

    val builder = new BCSCM.Builder[V](numRows, ncs)

    storage.collect().foreach { p =>
      val cidx = p.cidx.toInt
      val col = p.cvec

      col.foreachPair((ridx, v) => builder.add(ridx, cidx, v))
    }

    builder.result()
  }

  def t: IndexedRowMatrix[V] =
    IndexedRowMatrix(numCols,
                     numRows,
                     storage.map(col => IndexedRow(col.cidx, col.cvec)))

  def *(m: IndexedRowMatrix[V]): BDM[V] = {
    require(numCols == m.numRows,
            s"Required matrix product, "
              + s"but the m1.numCols = $numCols and m2.numRows = ${m.numRows}")

    val cols = storage.map(col => (col.cidx, col.cvec))
    val rows = m.mapStorage(row => (row.ridx, row.rvec))

    cols
      .join(rows)
      .map(x => x._2._1 * x._2._2.t)
      .fold(BDM.zeros[V](numRows, m.numCols))(_ + _)
  }
}

object IndexedColumnMatrix {
  def zeros[V: ClassTag: Zero: Semiring](numRows: Int, numCols: Long)(
      implicit sc: SparkContext): IndexedColumnMatrix[V] =
    IndexedColumnMatrix(numRows, numCols, sc.emptyRDD[IndexedColumn[V]])

  def rand[V: ClassTag: Zero: Semiring](numRows: Int,
                                        numCols: Long,
                                        rand: Rand[V] = Rand.uniform)(
      implicit sc: SparkContext): IndexedColumnMatrix[V] = {
    val cols = for { cidx <- 0L until numRows } yield
      IndexedColumn(cidx, BDV.rand[V](numRows, rand))

    IndexedColumnMatrix(numRows, numCols, sc.parallelize(cols))
  }

  def fromSeq[V, R](cols: R*)(implicit cl: LiteralRow[R, V],
                              man: ClassTag[V],
                              zero: Zero[V],
                              semiring: Semiring[V],
                              n: Numeric[V],
                              sc: SparkContext): IndexedColumnMatrix[V] = {
    val nrows = cl.length(cols(0))
    val ncols = cols.length

    val c = cols.zipWithIndex.map { p =>
      val col = p._1
      val cidx = p._2

      val builder = new BVB[V](nrows)
      cl.foreach(col, { (ridx, v) =>
        if (v != n.zero) builder.add(ridx, v)
      })
      IndexedColumn(cidx, builder.toDenseVector)
    }

    IndexedColumnMatrix(nrows, ncols, sc.parallelize(c))
  }

  def apply[V: ClassTag: Zero: Semiring](
      numRows: Int,
      numCols: Long,
      rdd: RDD[IndexedColumn[V]]): IndexedColumnMatrix[V] =
    new IndexedColumnMatrix[V](numRows, numCols, rdd)
}
