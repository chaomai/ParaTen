package org.chaomai.paraten.matrix

import breeze.linalg.{*, norm, DenseMatrix}
import org.chaomai.paraten.tensor.Coordinate
import org.chaomai.paraten.{Common, UnitSpec}

/**
  * Created by chaomai on 02/05/2017.
  */
class TestCoordinateMatrix extends UnitSpec {
  "Object CoordinateMatrix" should "build zeros matrix" in {
    implicit val sc = Common.sparkContext
    val m = CoordinateMatrix.zeros[Double](5, 4)
    println(m)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build rand matrix" in {
    implicit val sc = Common.sparkContext
    val m = CoordinateMatrix.rand[Double](5, 4)
    println(m)
    println(m.toDenseMatrix)
    println(m.toCSCMatrix)
  }

  it should "build from seq" in {
    implicit val sc = Common.sparkContext
    val m = CoordinateMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(0, 6))
    println(m)

    val e = m.find(_.coordinate == Coordinate(2, 1))
    assert(e.isDefined)
    assert(e.get.value == 6)

    println(m.toDenseMatrix)
    assert(m.toDenseMatrix == DenseMatrix((1, 2), (3, 4), (0, 6)))

    println(m.toCSCMatrix)
  }

  it should "not compile on empty type" in {
    assertDoesNotCompile(
      "implicit val sc = Common.sparkContext; CoordinateMatrix.zeros(5, 4);")
  }

  it should "not compile on wrong type" in {
    assertDoesNotCompile(
      "implicit val sc = Common.sparkContext; CoordinateMatrix.rand[Int](5, 4);")
  }

  it should "not compile on wrong seq" in {
    assertDoesNotCompile(
      "implicit val sc = Common.sparkContext; CoordinateMatrix.fromSeq((1, 2), (3))")
    assertDoesNotCompile(
      "implicit val sc = Common.sparkContext; CoordinateMatrix.fromSeq((1, 2), (3, 4, 5))")
  }

  "CoordinateMatrix" should "get row" in {
    implicit val sc = Common.sparkContext
    val m = CoordinateMatrix.rand[Double](5, 4)
    println(m)
    println(m.rowAt(3))
    println(m.localRowAt(3))
  }

  it should "perform tProd" in {
    implicit val sc = Common.sparkContext
    val m = CoordinateMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))

    val p = m.tProd
    println(p)
    assert(p == DenseMatrix((35, 44), (44, 56)))
  }

  it should "perform matrix product with DenseMatrix" in {
    implicit val sc = Common.sparkContext
    val m = CoordinateMatrix.vals(Seq(1, 2), Seq(3, 4), Seq(5, 6))
    val dm = DenseMatrix((1, 2), (3, 4))

    val p = m * dm
    println(p)

    assert(p.toDenseMatrix == DenseMatrix((7, 10), (15, 22), (23, 34)))
  }

  it should "perform normalization" in {
    implicit val sc = Common.sparkContext
    val m = CoordinateMatrix.vals(Seq(1.0, 2.0), Seq(3.0, 4.0), Seq(5.0, 6.0))

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
}
