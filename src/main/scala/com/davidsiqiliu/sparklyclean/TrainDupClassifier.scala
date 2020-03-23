package com.davidsiqiliu.sparklyclean

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
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

  def trainGradientBoostedTrees(data: Dataset[Row]): PipelineModel = {
    // GBT
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setMaxDepth(3)
      .setFeatureSubsetStrategy("auto")
      .setSeed(647)

    // Chain indexers, GBT and converter in a Pipeline
    val pipeline = new Pipeline().setStages(Array(gbt))

    // Split into training (70%) and testing (30%) sets
    val Array(training, testing) = data.randomSplit(Array(0.7, 0.3), seed = 647)

    // Train model
    val model = pipeline.fit(training)

    // Make predictions
    val predictions = model.transform(testing)

    // Evaluate
    val TP = predictions.select("label", "prediction").filter("label = 0 and prediction = 0").count
    val TN = predictions.select("label", "prediction").filter("label = 1 and prediction = 1").count
    val FP = predictions.select("label", "prediction").filter("label = 0 and prediction = 1").count
    val FN = predictions.select("label", "prediction").filter("label = 1 and prediction = 0").count
    val total = predictions.select("label").count.toDouble

    val confusionMatrix: Matrix = Matrices.dense(2, 2, Array(TP, FN, FP, TN))
    log.info("\nConfusion Matrix:\n" + confusionMatrix)

    val accuracy = (TP + TN) / total
    val precision = (TP + FP) / total
    val recall = (TP + FN) / total
    val F1 = 2 / (1 / precision + 1 / recall)
    log.info("\nAccuracy:\n" + accuracy)
    log.info("\nPrecision:\n" + precision)
    log.info("\nRecall:\n" + recall)
    log.info("\nF1:\n" + F1)

    // Return learned model
    model
  }

  def main(argv: Array[String]): Unit = {

    val args = new TrainDupClassifierConf(argv)

    val conf = new SparkConf().setAppName("SparklyClean - TrainDupClassifier")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.getOrCreate
    import sparkSession.implicits._

    // Read in labeled points
    val data = sc.textFile(args.input())
      .map(
        line => {
          // [t1Id, t2Id, label, feature1, feature2, ...]
          val tokens: Array[String] = line.split(",")
          val pair: String = s"(${tokens(0)},${tokens(1)})"
          val label: Double = tokens(2).toDouble
          val features: Array[Double] = tokens.slice(3, tokens.length).map(_.toDouble)

          (pair, label, Vectors.dense(features))
        }
      ).toDF("id", "label", "features")
    log.info("\nInput: " + args.input())

    // Train a pipeline and model
    val pipelineModel = trainGradientBoostedTrees(data)

    // Output model specifics
    val model = pipelineModel.stages(0).asInstanceOf[GBTClassificationModel]
    log.info("\nLearned GradientBoostedTrees model:\n" + model.toDebugString)

    // Save pipeline and model
    if (args.model() != "") {
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.model()), true)
      pipelineModel.write.overwrite().save(args.model())
      log.info("\nSaved model: " + args.model())
    }

    sc.stop()
  }
}
