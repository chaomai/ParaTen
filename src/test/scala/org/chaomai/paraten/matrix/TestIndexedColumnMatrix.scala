package org.chaomai.paraten.matrix

import breeze.linalg.DenseMatrix
import org.chaomai.paraten.{Common, UnitSpec}

/**
  * Created by chaomai on 11/05/2017.
  */
class TestIndexedColumnMatrix extends UnitSpec {
  "Object IndexedColumnMatrix" should "build zeros matrix" in {
    implicit val sc = Common.sc
    val m = IndexedColumnMatrix.zeros[Double](5, 4)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build rand matrix" in {
    implicit val sc = Common.sc
    val m = IndexedColumnMatrix.rand[Double](5, 4)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build from seq" in {
    implicit val sc = Common.sc
    val m = IndexedColumnMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    println(m.toDenseMatrix)
    println(m.toCSCMatrix)

    assert(m.toDenseMatrix == DenseMatrix((1, 3, 0), (2, 4, 6)))
    assert(m.numRows == 2)
    assert(m.numCols == 3)
  }

  "A IndexedColumnMatrix" should "perform transformation" in {
    implicit val sc = Common.sc
    val m = IndexedColumnMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))

    val mt = m.t

    println(mt.toDenseMatrix)
    println(mt.toCSCMatrix)

    assert(mt.numRows == 3)
    assert(mt.numCols == 2)
    assert(mt.toDenseMatrix == DenseMatrix((1, 2), (3, 4), (0, 6)))
  }

  it should "perform matrix with transformed IndexedColumnMatrix" in {
    implicit val sc = Common.sc
    val m = IndexedColumnMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))

    val p = m * m.t

    println(p)

    assert(p == DenseMatrix((35, 44), (44, 56)))
  }

  it should "perform matrix with zero-valued IndexedRowMatrix" in {
    implicit val sc = Common.sc
    val m = IndexedColumnMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))
    val m1 = IndexedRowMatrix.zeros[Int](3, 2)

    val p = m * m1

    println(p)

    assert(p == DenseMatrix((0, 0), (0, 0)))
  }
}
