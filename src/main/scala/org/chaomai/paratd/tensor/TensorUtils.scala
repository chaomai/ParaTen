package org.chaomai.paratd.tensor

import breeze.linalg.{all, diag, DenseMatrix}

/**
  * Created by chaomai on 17/04/2017.
  */
object TensorUtils {
  // A^t * A = I
  def isConverged(A: CoordinateMatrix[Double],
                  B: CoordinateMatrix[Double],
                  dotProductThreshold: Double = 0.99): Boolean = ???
//    all(diag(A.t * B) :> dotProductThreshold)
}
