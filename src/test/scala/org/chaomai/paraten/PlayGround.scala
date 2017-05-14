package org.chaomai.paraten

/**
  * Created by chaomai on 14/05/2017.
  */
object PlayGround {

  def main(args: Array[String]): Unit = {
    Tup((1, 2), (3, 4), (0, 6))
  }

  class Tup

  object Tup {
    def apply[V](vs: Seq[V]*): Unit = vs.foreach(println)
  }

  implicit def tupleToSeq[V](t: Product): Seq[V] = {
    var s = Seq[V]()

    for (v <- t.productIterator) {
      s = v.asInstanceOf[V] +: s
    }
    s
  }
}
