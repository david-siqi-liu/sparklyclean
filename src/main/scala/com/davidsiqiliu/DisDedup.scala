/**
 * Spark implementation of Distributed Data Deduplication (http://www.vldb.org/pvldb/vol9/p864-chu.pdf)
 */

package com.davidsiqiliu

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext, Partitioner}

import scala.util.Random
import scala.collection.mutable.ArrayBuffer

object DisDedup {
  val log: Logger = Logger.getLogger(getClass.getName)

  def computeSimilarity(t1: Array[String], t2: Array[String]): Double = {
    if (t1(0).split("-")(1) == t2(0).split("-")(1)){
      1.0
    } else {
      0.001
    }
  }

  def DisDedupMapper(tuple: String, l: Int, rand: Random): List[(Int, (String, String))] = {
    // Generate anchor from [1, l]
    val a = rand.nextInt(l) + 1
    // Output
    val pairs: ArrayBuffer[(Int, (String, String))] = ArrayBuffer()
    // LEFT
    for (p <- 1 until a) {
      val rid = (2 * l - p + 2) * (p - 1) / 2 + (a - p)
      pairs += ((rid, ("L", tuple)))
    }
    // SELF
    val rid = (2 * l - a + 2) * (a - 1) / 2
    pairs += ((rid, ("S", tuple)))
    // RIGHT
    for (q <- a + 1 to l) {
      val rid = (2 * l - a + 2) * (a - 1) / 2 + (q - a)
      pairs += ((rid, ("R", tuple)))
    }
    pairs.toList
  }

  class DisDedupPartitioner(numReducers: Int) extends Partitioner {
    def numPartitions: Int = numReducers
    def getPartition(rid: Any): Int = rid.hashCode % numPartitions
  }

  def DisDedupReducer(idx: Int, iter: Iterator[(Int, (String, String))]): Iterator[(Double, (String, String))] = {
    val leftTuples: ArrayBuffer[String] = ArrayBuffer()
    val selfTuples: ArrayBuffer[String] = ArrayBuffer()
    val rightTuples: ArrayBuffer[String] = ArrayBuffer()

    while (iter.hasNext) {
      val pair = iter.next
      // Parse
      val side = pair._2._1
      val tuple = pair._2._2
      // Partition into lists
      if (side == "L") {
        leftTuples += tuple
      } else if (side == "R") {
        rightTuples += tuple
      } else {
        selfTuples += tuple
      }
    }

    val duplicates: ArrayBuffer[(Double, (String, String))] = ArrayBuffer()
    if (leftTuples.nonEmpty && rightTuples.nonEmpty) {
      for (i <- leftTuples.indices; j <- rightTuples.indices) {
        val t1 = leftTuples(i).split(",")
        val t2 = rightTuples(j).split(",")
        duplicates += ((computeSimilarity(t1, t2), (t1(0), t2(0))))
      }
    } else {
      for (i <- selfTuples.indices; j <- selfTuples.indices) {
        if (i != j) {
          val t1 = selfTuples(i).split(",")
          val t2 = selfTuples(j).split(",")
            duplicates += ((computeSimilarity(t1, t2), (t1(0), t2(0))))
        }
      }
    }

    duplicates.iterator
  }

  def main(argv: Array[String]): Unit = {

    val args = new DisDedupConf(argv)

    val sparkConf = new SparkConf().setAppName("DisDedup")
    val sparkContext = new SparkContext(sparkConf)

    // Read in input file
    var inputFile = sparkContext.emptyRDD[String]
    if (args.header()) {
      inputFile = sparkContext
        .textFile(args.input())
        .mapPartitionsWithIndex {
          (idx, iter) => if (idx == 0) iter.drop(1) else iter
        }
    } else {
      inputFile = sparkContext.textFile(args.input())
    }
    log.info("Input: " + args.input())

    // Triangle distribution
    var l = math.floor(math.sqrt(2 * args.reducers())).toInt
    var numReducer = l * (l + 1) / 2
    if (numReducer > args.reducers()) {
      l = l - 1
      numReducer = l * (l + 1) / 2
    }
    log.info("Number of reducers used: " + numReducer)

    // Initialize random number generator
    val rand = new Random(seed = 647)

    // Map
    val inputRDD = inputFile
      .flatMap(tuple => DisDedupMapper(tuple, l, rand))

    // Partition
    val partitionedRDD = inputRDD
      .partitionBy(new DisDedupPartitioner(numReducer))

    // Similarity score threshold
    val threshold = args.threshold()

    // Reduce
    val outputRDD = partitionedRDD
      .mapPartitionsWithIndex((idx, iter) => DisDedupReducer(idx, iter))
      .filter{
        case (score, (t1, t2)) => score >= threshold
      }

    if (args.output() != "") {
      FileSystem.get(sparkContext.hadoopConfiguration).delete(new Path(args.output()), true)
      if (args.coalesce()){
        outputRDD.coalesce(1, shuffle = true).saveAsTextFile(args.output())
      } else {
        outputRDD.saveAsTextFile(args.output())
      }
      log.info("Output: " + args.output())
    }

    sparkContext.stop()
  }
}