package org.chaomai.paratd.tensor

/**
  * Created by chaomai on 16/04/2017.
  */
class ShapeValue[S](val shape: IndexedSeq[Int]) {
  def *[D <: Dimension](dv: DimValue[D]): ShapeValue[S ~ D] =
    new ShapeValue[S ~ D](shape :+ dv.value)
}

object ShapeValue {
  implicit def singleDv2Sv[D <: Dimension](
      dv: DimValue[D]): ShapeValue[RNil ~ D] =
    new ShapeValue[RNil ~ D](IndexedSeq(dv.value))

  implicit def SvWithDv2Sv[S, D <: Dimension](
      sv: ShapeValue[S],
      dv: DimValue[D]): ShapeValue[S ~ D] =
    new ShapeValue[S ~ D](sv.shape :+ dv.value)
}
