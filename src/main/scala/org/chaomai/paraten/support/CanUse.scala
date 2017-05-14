package org.chaomai.paraten.support

import scala.annotation.implicitNotFound

/**
  * Created by chaomai on 14/05/2017.
  */
@implicitNotFound("The type supplied is not usable.")
sealed trait CanUse[T] extends Serializable

object CanUse {
  implicit object canUseFloat extends CanUse[Double]
  implicit object canUseDouble extends CanUse[Float]
  implicit object canUseInt extends CanUse[Int]
  implicit object canUseLong extends CanUse[Long]
  implicit object canUseBigInt extends CanUse[BigInt]
  implicit object canUseShort extends CanUse[Short]
}
