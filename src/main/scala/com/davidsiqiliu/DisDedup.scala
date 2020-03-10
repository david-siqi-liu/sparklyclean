/**
 * Spark implementation of Distributed Data Deduplication (http://www.vldb.org/pvldb/vol9/p864-chu.pdf)
 */

package com.davidsiqiliu

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

object DisDedup {
  val log: Logger = Logger.getLogger(getClass.getName)

  def getBKV(tuple: String): String = {
    val BKV_POS = 13

    tuple.split(",")(BKV_POS).trim()
  }

  def computeSimilarity(t1: Array[String], t2: Array[String]): Double = {
    if (t1(0).split("-")(1) == t2(0).split("-")(1)) {
      1.0
    } else {
      0.001
    }
  }

  def setup(inputFile: RDD[String], k: Int, rand: Random): Map[String, List[Int]] = {
    // Count each block's size
    val sizeByBlock = inputFile
      .map {
        tuple => (getBKV(tuple), 1)
      }
      .reduceByKey(_ + _)

    // Calculate the total workload
    val numBlocks = sizeByBlock
      .count()

    val workByBlock = sizeByBlock
      .mapValues {
        n => n * (n - 1) / 2
      }

    val workTotal = workByBlock
      .map(_._2)
      .sum

    // Partition workByBlock into the following HashMaps based on workload
    //  1. Multi-reducer blocks
    //  2. Single-reducer blocks, larger than tau (for deterministic distribution)
    //  3. Single-reducer blocks, smaller than tau (for randomized distribution)
    val thresholdMultiSingle = workTotal.toDouble / k.toDouble
    val tau = workTotal.toDouble / (3 * k * math.log(k))
    val hmMulti = workByBlock
      .filter {
        case (bkv, w) => w > thresholdMultiSingle
      }
      .collectAsMap()


    val hmSingleD = workByBlock
      .filter {
        case (bkv, w) => w <= thresholdMultiSingle && w > tau
      }
      .sortBy(_._2)
      .collectAsMap()

    val hmSingleR = workByBlock
      .filter {
        case (bkv, w) => w <= tau
      }
      .collectAsMap()

    assert(hmMulti.size + hmSingleD.size + hmSingleR.size == numBlocks)

    // Initialize return HashMap
    val hmBKV2RID = Map[String, List[Int]]()

    // Distribute multi-reducer blocks
    val workMulti = hmMulti.values.sum
    val s = rand.shuffle((1 to k).toList)
    var n = 0
    var k_i = 0
    for ((bkv, w) <- hmMulti) {
      k_i = math.min(w, math.floor(w.toDouble / workMulti.toDouble * k.toDouble).toInt)
      hmBKV2RID(bkv) = s.slice(n, n + k_i)
      n += k_i
    }

    // Distribute large single-reducer blocks, deterministically in a round-robin fashion
    // However, unlike in the paper, we will continue to use the leftover reducers from
    // multi-reducer distribution to avoid un-used reducers
    for ((bkv, w) <- hmSingleD) {
      hmBKV2RID(bkv) = s.slice(n % k, n % k + 1)
      n += 1
    }

    // Distribute small single-reducer blocks, randomly
    for ((bkv, w) <- hmSingleR) {
      hmBKV2RID(bkv) = List(rand.nextInt(k) + 1)
    }

    hmBKV2RID
  }

  def getL(k_i: Int): Int = {
    val l_i = math.floor(math.sqrt(2 * k_i)).toInt
    if (l_i * (l_i + 1) / 2 <= k_i) {
      l_i
    } else {
      l_i - 1
    }
  }

  def disDedupMapper(tuple: String, bkv: String, rids: List[Int], k: Int, rand: Random):
  List[((Int, String), (String, String))] = {
    val k_i = rids.size
    // Single-reducer block
    if (k_i == 1) {
      List(((rids.head, bkv), ("S", tuple)))
    } else {
      // Multi-reducer block, triangle distribution
      // Largest integer s.t. l_i(l_i + 1) / 2 <= k_i (error in the paper)
      val l_i = getL(k_i)
      // Generate anchor from [1, l_i]
      val a = rand.nextInt(l_i) + 1
      // Output
      val pairs: ArrayBuffer[((Int, String), (String, String))] = ArrayBuffer()
      var ridIndex = 0
      var rid = 0
      // LEFT
      for (p <- 1 until a) {
        ridIndex = (2 * l_i - p + 2) * (p - 1) / 2 + (a - p)
        rid = rids(ridIndex)
        pairs += (((rid, bkv), ("L", tuple)))
      }
      // SELF
      ridIndex = (2 * l_i - a + 2) * (a - 1) / 2
      rid = rids(ridIndex)
      pairs += (((rid, bkv), ("S", tuple)))
      // RIGHT
      for (q <- a + 1 to l_i) {
        ridIndex = (2 * l_i - a + 2) * (a - 1) / 2 + (q - a)
        rid = rids(ridIndex)
        pairs += (((rid, bkv), ("R", tuple)))
      }
      pairs.toList
    }
  }

  class DisDedupPartitioner(numReducers: Int) extends Partitioner {
    def numPartitions: Int = numReducers

    def getPartition(ridbkv: Any): Int = (ridbkv.asInstanceOf[(Int, String)]._1 - 1).hashCode % numPartitions
  }

  def compareWithinBlock(leftTuples: ArrayBuffer[String], selfTuples: ArrayBuffer[String], rightTuples: ArrayBuffer[String]):
  ArrayBuffer[(Double, (String, String))] = {
    val similarities: ArrayBuffer[(Double, (String, String))] = ArrayBuffer()

    if (leftTuples.nonEmpty && rightTuples.nonEmpty) {
      for (i <- leftTuples.indices; j <- rightTuples.indices) {
        val t1 = leftTuples(i).split(",")
        val t2 = rightTuples(j).split(",")
        similarities += ((computeSimilarity(t1, t2), (t1(0), t2(0))))
      }
    } else {
      for (i <- selfTuples.indices; j <- selfTuples.indices) {
        if (i < j) {
          val t1 = selfTuples(i).split(",")
          val t2 = selfTuples(j).split(",")
          similarities += ((computeSimilarity(t1, t2), (t1(0), t2(0))))
        }
      }
    }

    similarities
  }

  def disDedupReducer(iter: Iterator[((Int, String), (String, String))]):
  Iterator[(Double, (String, String))] = {
    var leftTuples: ArrayBuffer[String] = ArrayBuffer()
    var selfTuples: ArrayBuffer[String] = ArrayBuffer()
    var rightTuples: ArrayBuffer[String] = ArrayBuffer()
    val similarities: ArrayBuffer[(Double, (String, String))] = ArrayBuffer()

    var pair = ((0, ""), ("", ""))
    var prevbkv = ""
    var bkv = ""
    var side = ""
    var tuple = ""
    while (iter.hasNext) {
      pair = iter.next
      // Parse
      bkv = pair._1._2
      side = pair._2._1
      tuple = pair._2._2
      // Same block
      if (bkv == prevbkv) {
        // Add into tuple lists
        if (side == "L") {
          leftTuples += tuple
        } else if (side == "R") {
          rightTuples += tuple
        } else {
          selfTuples += tuple
        }
      }
      // New block
      else {
        // Conduct comparison for the previous block
        similarities ++= compareWithinBlock(leftTuples, selfTuples, rightTuples)
        // Reset tuple lists
        if (side == "L") {
          leftTuples = ArrayBuffer[String](tuple)
        } else if (side == "R") {
          rightTuples = ArrayBuffer[String](tuple)
        } else {
          selfTuples = ArrayBuffer[String](tuple)
        }
        // Set prevbkv to bkv
        prevbkv = bkv
      }
    }

    // Conduct comparison for the last block
    similarities ++= compareWithinBlock(leftTuples, selfTuples, rightTuples)

    similarities.iterator
  }

  def main(argv: Array[String]): Unit = {

    val args = new DisDedupConf(argv)

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
    val hmBKV2RID = sc.broadcast(setup(inputFile, k, rand))
    log.info("\nhmBKV2RID: " + hmBKV2RID.value.toString())

    // Map
    val inputRDD = inputFile
      .flatMap(tuple => {
        val bkv = getBKV(tuple)
        val rids = hmBKV2RID.value(bkv)
        disDedupMapper(tuple, bkv, rids, k, rand)
      })

    // Partition
    val partitionedRDD = inputRDD
      .repartitionAndSortWithinPartitions(new DisDedupPartitioner(k))

    // Similarity score threshold
    val threshold = args.threshold()

    // Reduce
    val outputRDD = partitionedRDD
      .mapPartitions(iter => disDedupReducer(iter))
      .filter {
        case (score, (t1, t2)) => score >= threshold
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