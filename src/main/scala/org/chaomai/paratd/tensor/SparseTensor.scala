package org.chaomai.paratd.tensor

/**
  * Created by chaomai on 02/05/2017.
  */
case class Coordinate(coordinate: IndexedSeq[Int]) {
  val length: Int = coordinate.length

  def dimAt(idx: Int): Int = coordinate(idx)

  def dimKept(idx: Int*): Coordinate =
    Coordinate {
      coordinate.zipWithIndex.filter(p => idx.contains(p._2)).map(_._1)
    }

  def dimWithout(idx: Int*): Coordinate =
    Coordinate {
      coordinate.zipWithIndex.filter(p => !idx.contains(p._2)).map(_._1)
    }

  def compose(dims: Int*): Coordinate = Coordinate {
    coordinate ++ dims.toIndexedSeq
  }

  def updated(idx: Int, elem: Int) = Coordinate(coordinate.updated(idx, elem))

  def apply(idx: Int): Int = coordinate(idx)

  override def toString: String = {
    coordinate.foldLeft("(") { (acc, e) =>
      (acc, e) match {
        case ("(", e) => acc + e.toString
        case _ => acc + ", " + e.toString
      }
    } + ")"
  }
}

object Coordinate {
  def apply(dims: Int*): Coordinate = new Coordinate(dims.toIndexedSeq)
}

sealed trait Entry[V] extends Serializable

object Entry {
  def TEntry2VEntryOnDim[V](dim: Int, e: TEntry[V]): VEntry[V] = {
    require(
      e.coordinate.length > dim,
      s"Requested transformation from TEntry to VEntry on dim $dim, "
        + s"but TEntry is a value on ${e.coordinate.length} dimensional space"
    )
    VEntry(e.coordinate(dim), e.value)
  }
}

case class TEntry[V](coordinate: Coordinate, value: V) extends Entry[V] {
  def dimAt(idx: Int): Int = coordinate(idx)

  def dimWithout(idx: Int): Coordinate = coordinate.dimWithout(idx)

  override def toString: String = {
    "%s @ %s".format(value.toString, coordinate)
  }
}

case class VEntry[V](coordinate: Int, value: V) extends Entry[V] {
  override def toString: String = "%s @ %d".format(value.toString, coordinate)
}
