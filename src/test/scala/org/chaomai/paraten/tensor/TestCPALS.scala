package org.chaomai.paraten.tensor

import org.chaomai.paraten.{Common, UnitSpec}

/**
  * Created by chaomai on 16/05/2017.
  */
class TestCPALS extends UnitSpec {
  "CPALS" should "perform cp decomposition on a dense tensor" in {
    implicit val sc = Common.sc
    sc.setLogLevel("WARN")

    val t = Common.dim4DenseTensor
    val rank = 5
    val tol = 0.1

    val (fms, l) = new CPALS()
      .setRank(rank)
      .setMaxIter(25)
      .setTol(tol)
      .setTries(3)
      .run(t)

    fms.foreach(e => println(e.toDenseMatrix))
    println(l)

    val t1 = CoordinateTensor.fromFacMats(t.shape, rank, fms, l)
    println(t1)

    assert(t.:~==(t1, tol))
  }

  it should "perform cp decomposition on a sparse tensor" in {
    implicit val sc = Common.sc
    sc.setLogLevel("WARN")

    val t = Common.dim4SparseTensor
    val rank = 8
    val tol = 1e-3

    val (fms, l) = new CPALS()
      .setRank(rank)
      .setMaxIter(25)
      .setTol(tol)
      .setTries(3)
      .run(t)

    fms.foreach(e => println(e.toDenseMatrix))
    println(l)

    val t1 = CoordinateTensor.fromFacMats(t.shape, rank, fms, l)
    println(t1)

    assert(t.:~==(t1, tol))
  }
}
