package org.chaomai.paratd.vector

import breeze.linalg.DenseVector
import org.chaomai.paratd.UnitSpec

/**
  * Created by chaomai on 11/05/2017.
  */
class TestLocalSparseVector extends UnitSpec {
  "A LocalSparseVector" should "build from seq and exclude zero value while building" in {
    val sv = LocalSparseVector.vals(1, 2, 0, 4, 0, 6)
    println(sv)
    println(sv.toSparseVector)
  }

  it should "perform dot product with same-sized CoordinateVector" in {
    val sv = LocalSparseVector.vals(1, 2, 3, 4, 5)
    val v = DenseVector(2, 3, 4, 5, 6)
    assert((sv >* v) == 70)
  }

  it should "not perform dot product with different-sized CoordinateVector" in {
    val sv = LocalSparseVector.vals(1, 2, 3, 4, 5)
    val v = DenseVector(2, 3, 4)
    assertThrows[IllegalArgumentException](sv >* v)
  }

  "A LocalSparseVector that has zero value" should
    "perform dot product with same-sized DenseVector" in {
    val sv = LocalSparseVector.vals(0, 2, 0, 4, 5)
    val v = DenseVector(2, 3, 4, 5, 6)
    assert((sv >* v) == 56)
  }
}
