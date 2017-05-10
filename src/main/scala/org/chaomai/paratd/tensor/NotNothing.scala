package org.chaomai.paratd.tensor

import scala.annotation.implicitNotFound

/**
  * Created by chaomai on 16/04/2017.
  */
@implicitNotFound("You must explicitly provide a type.")
trait NotNothing[T]

object NotNothing {
  private val evidence: NotNothing[Any] = new Object with NotNothing[Any]
  implicit def notNothingEv[T](implicit n: T =:= T): NotNothing[T] =
    evidence.asInstanceOf[NotNothing[T]]
}
