package org.chaomai.paraten.apps

import org.apache.spark.{SparkConf, SparkContext}
import org.chaomai.paraten.tensor.{CPALS, CoordinateTensor}
import scopt.OptionParser

/**
  * Created by chaomai on 01/05/2017.
  */
/***
  * CP Decomposition via ALS on Spark.
  * The algorithm used is based on naive CP-ALS, adapted for the RDD.
  */
object ParaCP {
  private case class Params(shape: IndexedSeq[Int] = IndexedSeq[Int](),
                            rank: Int = 3,
                            maxIter: Int = 500,
                            tolerance: Double = 1e-3,
                            tries: Int = 3,
                            input: String = "",
                            outputDir: String = "")
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ParaCP") {
      head("CP decomposition on Spark")

      opt[String]('s', "shape")
        .required()
        .unbounded()
        .text("shape")
        .action((x, c) => c.copy(shape = x.split(',').map(_.toInt)))

      opt[Int]('r', "rank")
        .required()
        .text("rank")
        .action((x, c) => c.copy(rank = x))
        .validate(x =>
          if (x > 0) success
          else failure("number of rank r must be positive."))

      opt[Int]("maxIter")
        .text(
          s"number of iterations of ALS. default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
        .validate(x =>
          if (x > 0) success
          else failure("max iterations must be positive."))

      opt[Double]("tol")
        .text(s"tolerance for the ALS. default: ${defaultParams.tolerance}")
        .action((x, c) => c.copy(tolerance = x))
        .validate(x =>
          if (x > 0.0) success
          else failure("tolerance must be positive."))

      opt[Int]("tries")
        .text("tries")
        .action((x, c) => c.copy(tries = x))
        .validate(x =>
          if (x > 0) success
          else failure("number of tries must be positive."))

      opt[String]('i', "input")
        .text("path of input file.")
        .required()
        .action((x, c) => c.copy(input = x))

      opt[String]('o', "output-dir")
        .valueName("<dir>")
        .text(s"output write path.")
        .action((x, c) => c.copy(outputDir = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => paracp(params)
      case None =>
        parser.showUsageAsError()
        sys.exit(1)
    }
  }

  private def paracp(params: Params): Unit = {
    println("Start CP decomposition")

    val startT = System.nanoTime()

    val conf = new SparkConf().setAppName("ParaTD")
    implicit val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val t = CoordinateTensor.fromFile(params.input, params.shape)

    val preprocessDur = (System.nanoTime() - startT) / 1e9

    println(s"Preprocess time spend: $preprocessDur")

    val (facmats, lambda) = new CPALS()
      .setRank(params.rank)
      .setMaxIter(params.maxIter)
      .setTol(params.tolerance)
      .setTries(params.tries)
      .run(t)

    for (idxedm <- facmats.zipWithIndex) {
      val path = s"${params.outputDir}/factor_matrix_${idxedm._2}"
      val m = idxedm._1
      m.mapStorage { row =>
          val vstr = row.rvec.foldLeft("") { (acc, v) =>
            (acc, v) match {
              case ("", _) => acc + v.toString
              case _ => acc + " " + v.toString
            }
          }
          row.ridx.toString + ": " + vstr
        }
        .saveAsTextFile(path)
    }

    val path = s"${params.outputDir}/lambda_vector"
    sc.parallelize(lambda.toArray.zipWithIndex.toSeq)
      .map(p => p._2 + ": " + p._1)
      .saveAsTextFile(path)

    val appDur = (System.nanoTime() - startT) / 1e9

    println(s"Total time: $appDur")

    sc.stop()
  }
}
