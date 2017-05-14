package org.chaomai.paraten.tensor

import org.chaomai.paraten.UnitSpec

/**
  * Created by chaomai on 08/05/2017.
  */
class TestCoordinate extends UnitSpec {
  "A Coordinate" should "get coordinate without some dimension" in {
    val c = Coordinate(0, 1, 2, 3, 4, 5, 6)
    assert(c.dimWithout(3, 4).coordinate == IndexedSeq(0, 1, 2, 5, 6))
  }

  it should "get coordinate with some dimension kept" in {
    val c = Coordinate(0, 1, 2, 3, 4, 5, 6)
    assert(c.dimKept(3, 4).coordinate == IndexedSeq(3, 4))
  }

  it should "compose with indexes" in {
    val c = Coordinate(0, 1, 2, 3)
    assert(c.appendDim(4, 5, 6).coordinate == IndexedSeq(0, 1, 2, 3, 4, 5, 6))
  }

  "A TEntry" should "not build entry with wrong type" in {
    assertDoesNotCompile("TEntry((1, 2, 3), \"wrong\")")
  }
}
