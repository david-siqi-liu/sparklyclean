package com.davidsiqiliu.sparklyclean

import com.davidsiqiliu.sparklyclean.impl._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SparklyCleanMain {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {

    val args = new Conf(argv)

    val conf = new SparkConf().setAppName("DisDedup")
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

    // Similarity score threshold
    val threshold = args.threshold()

    // Reduce
    val outputRDD = partitionedRDD
      .mapPartitions(iter => DisDedupReducer.reduce(iter))
      .filter {
        case (score, (_, _)) => score >= threshold
      }

    if (args.output() != "") {
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)
      if (args.coalesce()) {
        outputRDD.coalesce(1, shuffle = true).saveAsTextFile(args.output())
      } else {
        inputRDD.saveAsTextFile(args.output() + "/mapper")
        partitionedRDD.saveAsTextFile(args.output() + "/partitioner")
        outputRDD.saveAsTextFile(args.output() + "/reducer")
        //        outputRDD.saveAsTextFile(args.output())
      }
      log.info("\nOutput: " + args.output())
    }

    sc.stop()
  }
}
