package org.chaomai.paraten.vector

import org.chaomai.paraten.{Common, UnitSpec}

/**
  * Created by chaomai on 02/05/2017.
  */
class TestCoordinateVector extends UnitSpec {
  "A CoordinateVector" should "mapPartitions" in {
    implicit val sc = Common.sc
    val v1 = CoordinateVector.vals(1, 2, 3, 4, 5)

    assert(v1.mapPartitions(es => es.map(_.value)).sum() == 15)
  }

  it should "perform outer product" in {
    implicit val sc = Common.sc
    val v1 = CoordinateVector.vals(1, 2, 3, 4, 5)
    val v2 = CoordinateVector.vals(1, 2, 3)
  }
}
