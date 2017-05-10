package org.chaomai.paratd

/**
  * Created by chaomai on 01/05/2017.
  */
package object tensor {
  type ~[S, D] = (S, D)

  val unitDim: DimValue[UnitDim] = DimValue[UnitDim](1)

  val rNilValue: DimValue[RNil] = null
}
