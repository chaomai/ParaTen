package org.chaomai.paratd.tensor

import breeze.linalg.DenseVector
import org.chaomai.paratd.{Common, UnitSpec}

/**
  * Created by chaomai on 02/05/2017.
  */
class TestCoordinateTensor extends UnitSpec {

  "A TensorFactory" should "build tensor from file" in {
    val dim = Common.sizeOfDim3Tensor
    val t = Common.dim3Tensor

    println(t)

    assert(t.dimension == dim.length)
    assert(t.size == dim.product)
    assert(t.shape == dim)
  }

  "A CoordinateTensor" should "get fiber from a dense tensor" in {
    val dim = Common.sizeOfDim4DenseTensor
    val t = Common.dim4DenseTensor

    println(t)

    assert(t.dimension == dim.length)
    assert(t.size == dim.product)
    assert(t.shape == dim)

    Common.debugMessage("fiber on mode 1")
    t.fibersOnMode(1).foreach(println)

    Common.debugMessage("fiber on mode 2")
    t.fibersOnMode(2).foreach(println)
  }

  "A CoordinateTensor" should "get fiber from a sparse tensor" in {
    val t = Common.dim4SparseTensor

    Common.debugMessage("fiber on mode 1")
    t.fibersOnMode(1).foreach(println)

    Common.debugMessage("fiber on mode 2")
    t.fibersOnMode(2).foreach(println)
  }

  it should "perform n-mode product" in {
    implicit val sc = Common.sparkContext

    val t = Common.dim4DenseTensor
    val v = sc.broadcast(DenseVector[Double](2, 2, 3))

    val nModeProd = t nModeProd (2, v.value)

    println(nModeProd)

    assert(nModeProd.shape == IndexedSeq(2, 2, 1, 2))

    val eOption =
      nModeProd.find(e => e.coordinate == Coordinate(IndexedSeq(1, 0, 0, 0)))
    assert(eOption.isDefined)
    assert(eOption.get.value == 107)
  }

  it should "perform cp decomposition on a dense tensor" in {
    implicit val sc = Common.sparkContext

    val t = Common.dim4DenseTensor
    val cpRet = CoordinateTensor.paraCP(t, 3, maxIter = 5, tries = 1)

    val fms = cpRet._1
    val l = cpRet._2

    fms.foreach(e => println(e.toDenseMatrix))
    println(l)
  }

  it should "perform cp decomposition on a sparse tensor" in {
    implicit val sc = Common.sparkContext

    val t = Common.dim4SparseTensor
    val cpRet = CoordinateTensor.paraCP(t, 3, maxIter = 5, tries = 1)

    val fms = cpRet._1
    val l = cpRet._2

    fms.foreach(e => println(e.toDenseMatrix))
    println(l)
  }
}
