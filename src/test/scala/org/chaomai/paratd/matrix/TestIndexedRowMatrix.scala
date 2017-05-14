package org.chaomai.paratd.matrix

import breeze.linalg.{*, norm, DenseMatrix, DenseVector}
import org.chaomai.paratd.{Common, UnitSpec}

/**
  * Created by chaomai on 11/05/2017.
  */
class TestIndexedRowMatrix extends UnitSpec {
  "Object IndexedRowMatrix" should "build zeros matrix" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.zeros[Double](5, 4)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build rand matrix" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.rand[Double](5, 4)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build from seq" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    assert(m.toDenseMatrix == DenseMatrix((1, 2), (3, 4), (0, 6)))
    assert(m.numRows == 3)
    assert(m.numCols == 2)

    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  "A IndexedRowMatrix" should "get column" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    assert(m.localColAt(1) == DenseVector(2, 4, 6))
  }

  it should "perform transformation" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    val mt = m.t
    assert(mt.numRows == 2)
    assert(mt.numCols == 3)
    assert(mt.toDenseMatrix == DenseMatrix((1, 2), (3, 4), (0, 6)).t)

    println(mt.toDenseMatrix)
    println(mt.toCSCMatrix)
  }

  it should "perform matrix product with DenseMatrix" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))
    val dm = DenseMatrix((1, 2), (3, 4))

    val p = m * dm

    assert(p.toDenseMatrix == DenseMatrix((7, 10), (15, 22), (23, 34)))

    println(p.toDenseMatrix)
  }

  it should "perform matrix product with transformed IndexedRowMatrix" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))

    val p = m * m.t

    assert(
      p.toDenseMatrix == DenseMatrix((5, 11, 17), (11, 25, 39), (17, 39, 61)))

    println(p.toDenseMatrix)
  }

  it should "perform transformed matrix product with IndexedRowMatrix" in {
    implicit val sc = Common.sparkContext
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))

    val p = m.t * m

    assert(p == DenseMatrix((35, 44), (44, 56)))

    println(p.toDenseMatrix)
  }

  it should "perform normalization" in {
    implicit val sc = Common.sparkContext
    val m =
      IndexedRowMatrix.vals(Seq(1.0, 2.0), Seq(3.0, 4.0), Seq(5.0, 6.0))

    val (nm, n) = m.normalizeByCol
    println(nm.toDenseMatrix)
    println(n)

    val n1 = norm(m.toDenseMatrix(::, *))
    val nm1 = m.toDenseMatrix

    for (i <- 0 until nm1.cols) {
      nm1(::, i) :/= n1(i)
    }

    assert(nm.toDenseMatrix == nm1)
    assert(n == n1.t)

    println(nm1)
    println(n1)
  }

  it should "check equality" in {
    implicit val sc = Common.sparkContext
    val m1 =
      IndexedRowMatrix.vals(Seq(0.1, 0.2), Seq(0.3, 0.4), Seq(0.5, 0.6))
    val m2 =
      IndexedRowMatrix.vals(Seq(0.1, 0.2), Seq(0.3, 0.3997), Seq(0.5, 0.6))
    val m3 =
      IndexedRowMatrix.vals(Seq(0.1, 0.2), Seq(0.3, 0.4), Seq(0.3, 0.6))

    assert(m1 ~= m2)
    assert(!(m1 ~= m3))
  }
}
