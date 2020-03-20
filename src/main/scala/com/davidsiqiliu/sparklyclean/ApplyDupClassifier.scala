package com.davidsiqiliu.sparklyclean

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ApplyDupClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input: ScallopOption[String] = opt[String](descr = "Path to testing labeled points", required = true)
  val output: ScallopOption[String] = opt[String](descr = "Path to output predictions", required = true)
  val model: ScallopOption[String] = opt[String](descr = "Path to trained model", required = true)
  verify()
}

object ApplyDupClassifier {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {

    val args = new ApplyDupClassifierConf(argv)

    val conf = new SparkConf().setAppName("SparklyClean - ApplyDupClassifier")
    val sc = new SparkContext(conf)

    // Read in labeled points
    val data = sc.textFile(args.input())
      .map(
        line => {
          // [t1, t2, label (if available), feature1, feature2, ...]
          val tokens: Array[String] = line.split(",")
          val t1: String = tokens(0)
          val t2: String = tokens(1)
          val features: Array[Double] = tokens.slice(3, tokens.length).map(_.toDouble)

          ((t1, t2), Vectors.dense(features))
        }
      )
    log.info("\nInput: " + args.input())

    // Load trained model
    val clf_nb = NaiveBayesModel.load(sc, args.model())

    // Perform prediction
    val predictions = data
      .map {
        case ((t1Id, t2Id), features) =>
          ((t1Id, t2Id), clf_nb.predict(features))
      }
      .sortBy(_._2)

    // Save predictions
    if (args.output() != "") {
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)
      predictions.saveAsTextFile(args.output())
      log.info("\nOutput: " + args.output())
    }

    sc.stop()
  }
}
