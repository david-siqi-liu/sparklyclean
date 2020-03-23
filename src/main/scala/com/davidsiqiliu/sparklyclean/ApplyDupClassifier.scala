package com.davidsiqiliu.sparklyclean

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ApplyDupClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input: ScallopOption[String] = opt[String](descr = "Path to testing labeled points", required = true)
  val output: ScallopOption[String] = opt[String](descr = "Path to output predictions", required = true)
  val model: ScallopOption[String] = opt[String](descr = "Path to trained models", required = true)
  verify()
}

object ApplyDupClassifier {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {

    val args = new ApplyDupClassifierConf(argv)

    val conf = new SparkConf().setAppName("SparklyClean - ApplyDupClassifier")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.getOrCreate
    import sparkSession.implicits._

    // Read in (non)labeled points
    val data = sc.textFile(args.input())
      .map(
        line => {
          // [t1, t2, _ (no label), feature1, feature2, ...]
          val tokens: Array[String] = line.split(",")
          val pair: String = s"(${tokens(0)},${tokens(1)})"
          val features: Array[Double] = tokens.slice(3, tokens.length).map(_.toDouble)

          (pair, Vectors.dense(features))
        }
      ).toDF("id", "features")
    log.info("\nInput: " + args.input())

    // Load and apply learned model
    val pipelineModel = PipelineModel.load(args.model())
    log.info("\nModel: " + args.model())

    val predictions = pipelineModel
      .transform(data)
      .select("id", "prediction")
      .rdd
      .map{
        case Row(id: String, prediction: Double) =>
          (id, prediction)
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
