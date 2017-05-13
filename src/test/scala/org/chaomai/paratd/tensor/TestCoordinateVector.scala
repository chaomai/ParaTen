package org.chaomai.paratd.tensor

import org.chaomai.paratd.vector.CoordinateVector
import org.chaomai.paratd.{Common, UnitSpec}

/**
  * Created by chaomai on 02/05/2017.
  */
class TestCoordinateVector extends UnitSpec {
  "A CoordinateVector" should "perform dot product" in {
    implicit val sc = Common.sparkContext
    val v1 = CoordinateVector.vals(1, 2, 3, 4, 5)
    val v2 = CoordinateVector.vals(1, 2, 3, 4, 5)

    v1.foreach(println)
    v2.foreach(println)

//    println(v1 >* v2)
  }

  it should "mapPartitions" in {
    implicit val sc = Common.sparkContext
    val v1 = CoordinateVector.vals(1, 2, 3, 4, 5)

    assert(v1.mapPartitions(es => es.map(_.value)).sum() == 15)
  }

  it should "perform outer product" in {
    implicit val sc = Common.sparkContext
    val v1 = CoordinateVector.vals(1, 2, 3, 4, 5)
    val v2 = CoordinateVector.vals(1, 2, 3)
  }
}
