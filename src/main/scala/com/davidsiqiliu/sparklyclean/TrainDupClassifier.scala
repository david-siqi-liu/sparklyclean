package com.davidsiqiliu.sparklyclean

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class TrainDupClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input: ScallopOption[String] = opt[String](descr = "Path to training labeled points", required = true)
  val model: ScallopOption[String] = opt[String](descr = "Path to output models", required = true)
  verify()
}

object TrainDupClassifier {
  val log: Logger = Logger.getLogger(getClass.getName)

  def trainNaiveBayes(training: RDD[LabeledPoint], testing: RDD[LabeledPoint]): NaiveBayesModel = {
    val clfNB = NaiveBayes.train(input = training, lambda = 1.0, modelType = "multinomial")

    // Perform predictions
    val predictionAndLabel = testing
      .map(
        p =>
          (clfNB.predict(p.features), p.label)
      )

    // Confusion matrix
    val metrics = new MulticlassMetrics(predictionAndLabel)
    log.info("\nNaiveBayes - Confusion Matrix:\n" + metrics.confusionMatrix)

    clfNB
  }

  def trainGradientBoostedTrees(training: RDD[LabeledPoint], testing: RDD[LabeledPoint]): GradientBoostedTreesModel = {
    // The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 100 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 5

    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val clfGBT = GradientBoostedTrees.train(training, boostingStrategy)

    // Perform predictions
    val predictionAndLabel = testing
      .map(
        p =>
          (clfGBT.predict(p.features), p.label)
      )

    // Confusion matrix
    val metrics = new MulticlassMetrics(predictionAndLabel)
    log.info("\nGradientBoostedTrees - Confusion Matrix:\n" + metrics.confusionMatrix)

    clfGBT
  }

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

    // Train a Naive Bayes model
    val clfNB = trainNaiveBayes(training, testing)

    // Train a GradientBoostedTrees model
    val clfGBT = trainGradientBoostedTrees(training, testing)

    // Save models
    if (args.model() != "") {
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.model()), true)
      clfNB.save(sc, args.model() + "/nb")
      clfGBT.save(sc, args.model() + "/gbt")
      log.info("\nModel: " + args.model())
    }

    sc.stop()
  }
}
