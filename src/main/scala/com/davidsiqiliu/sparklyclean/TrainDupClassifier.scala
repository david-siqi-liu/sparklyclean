package com.davidsiqiliu.sparklyclean

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class TrainDupClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input: ScallopOption[String] = opt[String](descr = "Path to training labeled points", required = true)
  val model: ScallopOption[String] = opt[String](descr = "Path to output model", required = true)
  verify()
}

object TrainDupClassifier {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {

    val args = new TrainDupClassifierConf(argv)

    val conf = new SparkConf().setAppName("SparklyClean - TrainDupClassifier")
    val sc = new SparkContext(conf)

    // Read in labeled points
    val data = sc.textFile(args.input())
      .map(
        line => {
          // [t1Id, t2Id, label, feature1, feature2, ...]
          val tokens: Array[String] = line.split(",")
          val label: Double = tokens(2).toDouble
          val features: Array[Double] = tokens.slice(3, tokens.length).map(_.toDouble)

          LabeledPoint(label, Vectors.dense(features))
        }
      )
    log.info("\nInput: " + args.input())

    // Split into training (70%) and testing (30%) sets
    val Array(training, testing) = data.randomSplit(Array(0.7, 0.3))

    // Train model on training set
    val clf_nb = NaiveBayes.train(input = training, lambda = 1.0, modelType = "multinomial")

    // Prediction on testing set
    val predictionAndLabel = testing
      .map(
        p =>
          (clf_nb.predict(p.features), p.label)
      )

    // Confusion matrix
    val metrics = new MulticlassMetrics(predictionAndLabel)
    log.info("\nConfusion matrix:\n" + metrics.confusionMatrix)

    // Save model
    if (args.model() != "") {
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.model()), true)
      clf_nb.save(sc, args.model())
      log.info("\nModel: " + args.model())
    }

    sc.stop()
  }
}
