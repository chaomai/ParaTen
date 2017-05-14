package org.chaomai.paraten

/**
  * Created by chaomai on 01/05/2017.
  */
object ParaTD {
  private case class Params(input: Seq[String],
                            rank: Int = 3,
                            maxIterations: Int = 500,
                            tolerance: Double = 1e-3,
                            outputDir: String = ".")

  def main(args: Array[String]): Unit = {}
}
