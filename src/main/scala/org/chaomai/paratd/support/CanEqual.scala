package org.chaomai.paratd.support

import scala.annotation.implicitNotFound

/**
  * Created by chaomai on 15/05/2017.
  */
@implicitNotFound("The type supplied is not applicable for checking equality.")
sealed trait CanEqual[T] extends Serializable

/***
  * checking equality of Double and Float is meaningless.
  */
object CanEqual {
  implicit object canEqualInt extends CanEqual[Int]
  implicit object canEqualLong extends CanEqual[Long]
  implicit object canEqualBigInt extends CanEqual[BigInt]
  implicit object canEqualShort extends CanEqual[Short]
}
