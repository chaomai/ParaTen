package org.chaomai.paraten.tensor

import org.chaomai.paraten.{Common, UnitSpec}

/**
  * Created by chaomai on 16/05/2017.
  */
class TestCPALS extends UnitSpec {
  it should "perform cp decomposition on a dense tensor" in {
    implicit val sc = Common.sc
    sc.setLogLevel("WARN")

    val t = Common.dim4DenseTensor
    val rank = 5
    val (fms, l) = new CPALS()
      .setRank(rank)
      .setMaxIter(25)
      .setTol(0.1)
      .setTries(2)
      .run(t)

    fms.foreach(e => println(e.toDenseMatrix))
    println(l)

    val t1 = CoordinateTensor.fromFacMats(t.shape, rank, fms, l)
    println(t1)

    assert(t.:~==(t1, 0.1))
  }

  it should "perform cp decomposition on a sparse tensor" in {
    implicit val sc = Common.sc
    sc.setLogLevel("WARN")

    val t = Common.dim4SparseTensor
    val rank = 8
    val (fms, l) = new CPALS()
      .setRank(rank)
      .setMaxIter(25)
      .setTol(0.1)
      .setTries(2)
      .run(t)

    fms.foreach(e => println(e.toDenseMatrix))
    println(l)

    val t1 = CoordinateTensor.fromFacMats(t.shape, rank, fms, l)
    println(t1)

    assert(t.:~==(t1, 0.1))
  }
}
