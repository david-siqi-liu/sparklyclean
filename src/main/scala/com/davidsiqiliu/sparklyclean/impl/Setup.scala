/**
 * Setup phase
 *  1. Apply blocking functions to each tuple to obtain BKVs
 *  2. Compute workload for each blocks
 *  3. Partition the blocks into the following groups based on their workload
 *    - Multi-reducer blocks
 *    - Single-reducer, deterministic blocks
 *    - Single-reducer, randomized blocks
 *  4. Assign reducer IDs to each group
 *    - For multi-reducer blocks, assign optimal number of workers based on the triangle distribution strategy
 *    - For single-reducer, deterministic blocks, assign one worker in a round-robin fashion
 *    - For single-reducer, randomized blocks, assign one worker randomly
 *  5. Return a mapping of BKV to reducer IDs (List of Ints)
 */

package com.davidsiqiliu.sparklyclean.impl

import com.davidsiqiliu.sparklyclean.impl.Util._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Setup {

  def setup(log: Logger, inputFile: RDD[String], k: Int, rand: Random): mutable.Map[BKV, List[Int]] = {
    // Apply blocking functions to each tuple
    // Count each block's size
    val sizeByBlock: RDD[(BKV, Long)] = inputFile
      .flatMap(
        tuple => {
          val bkvs: ArrayBuffer[(BKV, Long)] = ArrayBuffer()
          for (b <- getBKVs(tuple)) {
            bkvs += ((b, 1))
          }
          bkvs.toList
        }
      )
      .reduceByKey(_ + _)

    // Number of blocks
    val numBlocks: Long = sizeByBlock
      .count()

    // Workload for each block
    val workByBlock: RDD[(BKV, Long)] = sizeByBlock
      .mapValues {
        n => n * (n - 1) / 2
      }

    // Total workload across all blocks
    val workTotal: Long = workByBlock
      .map(_._2)
      .sum
      .toLong

    // Compute threshold for multi vs. single
    val thresholdMultiSingle: Double = workTotal.toDouble / k.toDouble
    // Compute threshold for single-deterministic vs. single-randomized
    val tau: Double = workTotal.toDouble / (3 * k * math.log(k))
    log.info("\nthresholdMultiSingle: " + thresholdMultiSingle.toString)
    log.info("\ntau: " + tau.toString)

    // Partition blocks based on workload
    // Multi-reducer blocks
    val hmMulti: collection.Map[BKV, Long] = workByBlock
      .filter {
        case (_, w) => w > thresholdMultiSingle
      }
      .collectAsMap()
    log.info("\nhmMulti:\n" + hmMulti.mkString("\n"))

    // Single-reducer, deterministic blocks
    val hmSingleD: collection.Map[BKV, Long] = workByBlock
      .filter {
        case (_, w) => w <= thresholdMultiSingle && w > tau
      }
      .sortBy(_._2)
      .collectAsMap()
    log.info("\nhmSingleD:\n" + hmSingleD.mkString("\n"))

    // Single-reducer, randomized blocks
    val hmSingleR: collection.Map[BKV, Long] = workByBlock
      .filter {
        case (_, w) => w <= tau
      }
      .collectAsMap()
    log.info("\nhmSingleR:\n" + hmSingleR.mkString("\n"))

    // Make sure all blocks are accounted for
    assert(hmMulti.size + hmSingleD.size + hmSingleR.size == numBlocks)

    // Initialize mapping for BKV to reducer ID
    val hmBKV2RID: mutable.Map[BKV, List[Int]] = mutable.Map[BKV, List[Int]]()

    // Distribute multi-reducer blocks
    // Total workload across all multi-reducer blocks
    val workMulti: Long = hmMulti.values.sum
    log.info("\nworkMulti: " + workMulti.toString)

    val hmMultiKi: mutable.Map[BKV, Int] = mutable.Map[BKV, Int]() // (bkv, number of reducers)
    var multiKiDiffsExtras: ArrayBuffer[(BKV, (Int, Int))] = new ArrayBuffer() // (bkv, (diff, extra))
    var k_i_orig: Int = 0
    var k_i: Int = 0
    var k_i_extra: Int = 0
    var l_i: Int = 0

    for ((bkv, w) <- hmMulti) {
      k_i_orig = math.floor(w.toDouble / workMulti.toDouble * k.toDouble).toInt // Based on the paper
      l_i = getL(k_i_orig) // l_i prior to optimization
      k_i = l_i * (l_i + 1) / 2 // k_i prior to optimization
      k_i_extra = (l_i + 1) * (l_i + 2) / 2 // k_i, if l_i increases by 1
      hmMultiKi(bkv) = k_i
      multiKiDiffsExtras += ((bkv, (k_i_orig - k_i, k_i_extra - k_i)))
    }
    log.info("\nhmMultiKi (prior optimization):\n" + hmMultiKi.mkString("\n"))

    // Filter out blocks that have exactly the number of workers they need
    // Sort by the difference value
    multiKiDiffsExtras = multiKiDiffsExtras
      .filter(_._2._1 > 0)
      .sortBy(_._2._1)(Ordering[Int].reverse)
    log.info("\nmultiKiDiffsExtras:\n" + multiKiDiffsExtras.mkString("\n"))

    // Total number of workers left-over
    var numKLeftovers: Int = k - hmMultiKi.values.sum
    // Distribute the left-over workers to blocks that need extra workers
    for ((bkv, (_, extra)) <- multiKiDiffsExtras) {
      if (extra <= numKLeftovers) {
        hmMultiKi(bkv) += extra // increment number of workers by the amount of extras needed
        numKLeftovers -= extra
      }
    }
    log.info("\nhmMultiKi (after optimization):\n" + hmMultiKi.mkString("\n"))

    // Shuffle all reducers (starting from 1)
    val s: List[Int] = rand.shuffle((1 to k).toList)
    log.info("\nShuffled s: " + s.toString())

    // Distribute workers to multi-reducer blocks, based on the number of workers previously determined
    var n: Int = 0
    for ((bkv, k_i) <- hmMultiKi) {
      hmBKV2RID(bkv) = s.slice(n, n + k_i)
      n += k_i
    }

    // Distribute workers to single-reducer, deterministic blocks
    // However, unlike in the paper, we will continue to use the leftover workers (if any) from
    // multi-reducer distribution to minimize un-used workers
    for ((bkv, _) <- hmSingleD) {
      hmBKV2RID(bkv) = s.slice(n % k, n % k + 1)
      n += 1
    }

    // Distribute workers to single-reducer, randomized blocks
    for ((bkv, _) <- hmSingleR) {
      hmBKV2RID(bkv) = List(rand.nextInt(k) + 1)
    }

    hmBKV2RID
  }
}
