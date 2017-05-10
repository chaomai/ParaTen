package org.chaomai.paratd.tensor

import org.chaomai.paratd.UnitSpec

/**
  * Created by chaomai on 16/04/2017.
  */
class TestDimension extends UnitSpec {

  "A DimValue" should "be build with explicit Dimension type" in {
    val dimValue = 1

    trait Dim1 extends VarDim

    assertCompiles("DimValue[VarDim](dimValue)")

    val dim1 = DimValue[VarDim](dimValue)

    assert(dim1.value == dimValue)
  }

  it should "compile when build without explicit Dimension type" in {
    assertDoesNotCompile("DimValue(1)")
  }
}
