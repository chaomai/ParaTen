package org.chaomai.paraten.tensor

import breeze.linalg.DenseVector
import org.chaomai.paraten.{Common, UnitSpec}

/**
  * Created by chaomai on 02/05/2017.
  */
class TestCoordinateTensor extends UnitSpec {
  "A TensorFactory" should "build tensor from file" in {
    val dim = Common.dim3TensorSize
    val t = Common.dim3Tensor

    println(t)

    assert(t.dimension == dim.length)
    assert(t.size == dim.product)
    assert(t.shape == dim)
  }

  it should "build tensor from vals" in {
    implicit val sc = Common.sc

    val es = Seq(
      TEntry((0, 0, 0, 0), 23.0),
      TEntry((0, 0, 0, 1), 65.0),
      TEntry((0, 1, 0, 0), 30.0),
      TEntry((0, 1, 0, 1), 72.0),
      TEntry((1, 0, 0, 0), 107.0),
      TEntry((1, 0, 0, 1), 149.0),
      TEntry((1, 1, 0, 0), 114.0),
      TEntry((1, 1, 0, 1), 156.0)
    )

    val t = CoordinateTensor.vals(IndexedSeq(2, 2, 1, 2), es: _*)

    println(t)
  }

  it should "build tensor from DenseVector" in {
    implicit val sc = Common.sc
    val v1 = DenseVector(1, 2, 3, 4, 5)
    val t = CoordinateTensor.fromDenseVector(v1.length, v1)

    println(t)
  }

  "A CoordinateTensor" should "get fiber from a dense tensor" in {
    val t = Common.dim4DenseTensor

    Common.debugMessage("fiber on mode 1")
    t.fibersOnMode(1).foreach(println)

    Common.debugMessage("fiber on mode 2")
    t.fibersOnMode(2).foreach(println)
  }

  it should "get fiber from a sparse tensor" in {
    val t = Common.dim4SparseTensor

    Common.debugMessage("fiber on mode 1")
    t.fibersOnMode(1).foreach(println)

    Common.debugMessage("fiber on mode 2")
    t.fibersOnMode(2).foreach(println)
  }

  it should "check tensor equality" in {
    implicit val sc = Common.sc
    val t = Common.dim4DenseTensor

    assert(t :~== t)
  }

  it should "perform tensor addition" in {
    implicit val sc = Common.sc
    val t = Common.dim4DenseTensor

    val t1 = t :+ 1
    val t2 = t.map(_.map(_ + 1))

    val t3 = t1 :+ t2
    val t4 = t.map(_.map(v => (v + 1) * 2))

    assert(t3 :~== t4)

    println(t3)
  }

  it should "perform elementwise addition with different-numbered entries" in {
    implicit val sc = Common.sc
    val t = Common.dim4DenseTensor

    val t1 = CoordinateTensor.vals(t.shape, TEntry((0, 0, 2, 0), 5.0))

    val t2 = t :+ t1

    val eOption =
      t2.find(e => e.coordinate == Coordinate(IndexedSeq(0, 0, 2, 0)))
    assert(eOption.isDefined)
    assert(eOption.get.value == 10)

    println(t2)
  }

  it should "perform scalar addition" in {
    val t = Common.dim4DenseTensor

    val t1 = t :+ 1
    val t2 = t.map(_.map(_ + 1))

    assert(t1 :~== t2)

    println(t1)
  }

  it should "perform scalar production" in {
    val t = Common.dim4DenseTensor

    val t1 = t :* 5
    val t2 = t.map(_.map(_ * 5))

    assert(t1 :~== t2)

    println(t1)
  }

  it should "perform outer product with vector" in {
    implicit val sc = Common.sc
    val v1 = DenseVector(1, 2, 3, 4, 5)
    val t = CoordinateTensor.fromDenseVector(v1.length, v1)

    val v2 = DenseVector(6, 7, 8, 9, 10)
    val t1 = t <* v2

    val outerv3 = v1 * v2.t
    val entries = outerv3
      .mapPairs { (coord, v) =>
        TEntry(coord, v)
      }
      .toArray
      .toSeq
    val t2 =
      CoordinateTensor.vals(IndexedSeq(v1.length, v2.length), entries: _*)

    println(t1)
    println(outerv3)

    assert(t1 :~== t2)
  }

  it should "perform n-mode product" in {
    implicit val sc = Common.sc
    val t = Common.dim4DenseTensor
    val v = sc.broadcast(DenseVector[Double](2, 2, 3))

    val nModeProd = t nModeProd (2, v.value)

    println(nModeProd)

    assert(nModeProd.shape == IndexedSeq(2, 2, 1, 2))

    val eOption =
      nModeProd.find(e => e.coordinate == Coordinate(IndexedSeq(1, 0, 0, 0)))
    assert(eOption.isDefined)
    assert(eOption.get.value == 107)

    val es = Seq(
      TEntry((0, 0, 0, 0), 23.0),
      TEntry((0, 0, 0, 1), 65.0),
      TEntry((0, 1, 0, 0), 30.0),
      TEntry((0, 1, 0, 1), 72.0),
      TEntry((1, 0, 0, 0), 107.0),
      TEntry((1, 0, 0, 1), 149.0),
      TEntry((1, 1, 0, 0), 114.0),
      TEntry((1, 1, 0, 1), 156.0)
    )

    val t1 = CoordinateTensor.vals(IndexedSeq(2, 2, 1, 2), es: _*)

    assert(nModeProd :~== t1)
  }
}
