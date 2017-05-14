package org.chaomai.paratd.support

import scala.annotation.implicitNotFound

/**
  * Created by chaomai on 15/05/2017.
  */
@implicitNotFound("The type supplied is not applicable for checking approximately equality.")
sealed trait CanApproximatelyEqual[T] extends Serializable

object CanApproximatelyEqual {
  implicit object canApproximatelyDouble extends CanApproximatelyEqual[Double]
  implicit object canApproximatelyFloat extends CanApproximatelyEqual[Float]
  implicit object canApproximatelyInt extends CanApproximatelyEqual[Int]
  implicit object canApproximatelyLong extends CanApproximatelyEqual[Long]
  implicit object canApproximatelyBigInt extends CanApproximatelyEqual[BigInt]
  implicit object canApproximatelyShort extends CanApproximatelyEqual[Short]
}
