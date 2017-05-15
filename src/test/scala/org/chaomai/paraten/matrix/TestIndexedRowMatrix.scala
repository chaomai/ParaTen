package org.chaomai.paraten.matrix

import breeze.linalg.{*, norm, DenseMatrix, DenseVector}
import org.chaomai.paraten.{Common, UnitSpec}

/**
  * Created by chaomai on 11/05/2017.
  */
class TestIndexedRowMatrix extends UnitSpec {
  "Object IndexedRowMatrix" should "build zeros matrix" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.zeros[Double](5, 4)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build rand matrix" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.rand[Double](5, 4)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build from seq" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    println(m.toDenseMatrix)
    println(m.toCSCMatrix)

    assert(m.toDenseMatrix == DenseMatrix((1, 2), (3, 4), (0, 6)))
    assert(m.numRows == 3)
    assert(m.numCols == 2)
  }

  "A IndexedRowMatrix" should "get column" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    assert(m.localColAt(1) == DenseVector(2, 4, 6))
  }

  it should "perform transformation" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    val mt = m.t

    println(mt.toDenseMatrix)
    println(mt.toCSCMatrix)

    assert(mt.numRows == 2)
    assert(mt.numCols == 3)
    assert(mt.toDenseMatrix == DenseMatrix((1, 2), (3, 4), (0, 6)).t)
  }

  it should "perform matrix product with DenseMatrix" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))
    val dm = DenseMatrix((1, 2), (3, 4))

    val p = m * dm

    println(p.toDenseMatrix)

    assert(p.toDenseMatrix == DenseMatrix((7, 10), (15, 22), (23, 34)))
  }

  it should "perform matrix product with transformed IndexedRowMatrix" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))

    val p = m * m.t

    println(p.toDenseMatrix)

    assert(
      p.toDenseMatrix == DenseMatrix((5, 11, 17), (11, 25, 39), (17, 39, 61)))
  }

  it should "perform transformed matrix product with IndexedRowMatrix" in {
    implicit val sc = Common.sc
    val m = IndexedRowMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))

    val p = m.t * m

    println(p.toDenseMatrix)

    assert(p == DenseMatrix((35, 44), (44, 56)))
  }

  it should "perform normalization" in {
    implicit val sc = Common.sc
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

    println(nm1)
    println(n1)

    assert(nm.toDenseMatrix == nm1)
    assert(n == n1.t)
  }

  it should "check equality" in {
    implicit val sc = Common.sc
    val m1 =
      IndexedRowMatrix.vals(Seq(0.1, 0.2), Seq(0.3, 0.4), Seq(0.5, 0.6))
    val m2 =
      IndexedRowMatrix.vals(Seq(0.1, 0.2), Seq(0.3, 0.3997), Seq(0.5, 0.6))
    val m3 =
      IndexedRowMatrix.vals(Seq(0.1, 0.2), Seq(0.3, 0.4), Seq(0.3, 0.6))
    val m4 = IndexedRowMatrix.zeros[Double](3, 2)

    assert(m1.:~==(m2, 1e-3))
    assert(!m1.:~==(m3, 1e-3))
    assert(!m1.:~==(m4, 1e-3))
    assert(m1.:~==(m4, 1))
  }
}
