package org.chaomai.paratd.vector

import org.chaomai.paratd.{Common, UnitSpec}

/**
  * Created by chaomai on 03/05/2017.
  */
class TestLocalCoordinateVector extends UnitSpec {
  "A LocalCoordinateVector" should "build vector from values" in {
    val v = LocalCoordinateVector.vals(1, 2, 3, 4, 5)
    println(v)
  }

  it should "exclude zero value while building vector" in {
    val v = LocalCoordinateVector.vals(0, 2, 0, 4, 5)
    println(v)
  }

  it should "perform dot product with same-sized CoordinateVector" in {
    implicit val sc = Common.sparkContext

    val vl = LocalCoordinateVector.vals(1, 2, 3, 4, 5)
    val v = sc.broadcast(CoordinateVector.vals(2, 3, 4, 5, 6))

    assert((vl >* v.value) == 70)
  }

  it should "perform dot product with same-sized CoordinateVector that has zero value" in {
    implicit val sc = Common.sparkContext

    val vl = LocalCoordinateVector.vals(1, 2, 3, 4, 5)
    val v = sc.broadcast(CoordinateVector.vals(2, 3, 0, 5, 0))

    assert((vl >* v.value) == 28)
  }

  it should "not perform dot product with different-sized CoordinateVector" in {
    implicit val sc = Common.sparkContext

    val vl = LocalCoordinateVector.vals(1, 2, 3)
    val v = sc.broadcast(CoordinateVector.vals(2, 3, 4, 5, 6))

    assertThrows[IllegalArgumentException](vl >* v.value)
  }

  it should "perform dot product with same-sized LocalCoordinateVector" in {
    val v1 = LocalCoordinateVector.vals(1, 2, 3, 4, 5)
    val v2 = LocalCoordinateVector.vals(2, 3, 4, 5, 6)

    assert((v1 >* v2) == 70)
  }

  it should "not perform dot product with different-sized LocalCoordinateVector" in {
    val v1 = LocalCoordinateVector.vals(1, 2, 3)
    val v2 = LocalCoordinateVector.vals(2, 3, 4, 5, 6)

    assertThrows[IllegalArgumentException](v1 >* v2)
  }

  "A LocalCoordinateVector that has zero value" should
    "perform dot product with same-sized CoordinateVector" in {
    implicit val sc = Common.sparkContext

    val vl = LocalCoordinateVector.vals(0, 2, 0, 4, 5)
    val v = sc.broadcast(CoordinateVector.vals(2, 3, 4, 5, 6))

    assert((vl >* v.value) == 56)
  }

  it should "perform dot product with same-sized CoordinateVector that has zero value" in {
    implicit val sc = Common.sparkContext

    val vl = LocalCoordinateVector.vals(0, 2, 0, 4, 5)
    val v = sc.broadcast(CoordinateVector.vals(2, 3, 0, 5, 0))

    assert((vl >* v.value) == 26)
  }
}
