package org.chaomai.paraten.tensor

import org.chaomai.paraten.support.NotNothing

/**
  * Created by chaomai on 16/04/2017.
  */
sealed trait Dimension

sealed trait UnitDim extends Dimension

trait VarDim extends Dimension

trait DimValue[D] {
  def value: Int

  def *[D1 <: Dimension](dv: DimValue[D1]): ShapeValue[RNil ~ D ~ D1] =
    new ShapeValue[RNil ~ D ~ D1](IndexedSeq(value) :+ dv.value)
}

object DimValue {
  def apply[D: NotNothing](v: Int) = new DimValue[D] {
    override def value: Int = v
  }
}

sealed trait RNil {
  def ~[A](a: A): (RNil, A) = (this, a)
}

case object RNil extends RNil
