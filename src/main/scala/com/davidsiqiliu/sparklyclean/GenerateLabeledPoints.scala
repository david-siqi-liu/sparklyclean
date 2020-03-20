package com.davidsiqiliu.sparklyclean

import com.davidsiqiliu.sparklyclean.impl._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class GenerateLabeledPointsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = false, default = Some(""))
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val header: ScallopOption[Boolean] = opt[Boolean]()
  val label: ScallopOption[Boolean] = opt[Boolean]()
  verify()
}

object GenerateLabeledPoints {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {

    val args = new GenerateLabeledPointsConf(argv)

    val conf = new SparkConf().setAppName("SparklyClean - GenerateLabeledPoints")
    val sc = new SparkContext(conf)

    // Read in input file
    var inputFile = sc.emptyRDD[String]
    if (args.header()) {
      inputFile = sc
        .textFile(args.input())
        .mapPartitionsWithIndex {
          (idx, iter) => if (idx == 0) iter.drop(1) else iter
        }
    } else {
      inputFile = sc.textFile(args.input())
    }
    log.info("\nInput: " + args.input())

    // Initialize random number generator
    val rand = new Random(seed = 647)

    // Get HashMap for bkv (block-key-value) to reducer ID
    val k = args.reducers()
    val hmBKV2RID = sc.broadcast(Setup.setup(inputFile, k, rand))
    log.info("\nhmBKV2RID: " + hmBKV2RID.value.toString())

    // Map
    val inputRDD = inputFile
      .flatMap(tuple => {
        val bkvs = Util.getBKVs(tuple)
        val output: ArrayBuffer[((Int, BKV), (String, String))] = new ArrayBuffer()
        for (bkv <- bkvs) {
          val rids = hmBKV2RID.value(bkv)
          output ++= DisDedupMapper.map(tuple, bkv, rids, k, rand)
        }
        output.toList
      })

    // Partition
    val partitionedRDD = inputRDD
      .repartitionAndSortWithinPartitions(new DisDedupPartitioner(k))

    // Reduce
    val label = args.label()
    val outputRDD = partitionedRDD
      .mapPartitions(iter => DisDedupReducer.reduce(label, iter))

    // Save generated points
    if (args.output() != "") {
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)
      outputRDD.saveAsTextFile(args.output())
      log.info("\nOutput: " + args.output())
    }

    sc.stop()
  }
}
