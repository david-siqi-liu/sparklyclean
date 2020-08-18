/**
 * Main class for generating labeled points of similarity features between two tuples
 *
 * Copyright (c) 2020 David Liu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

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

    // Read in input file, skip first line if there's header
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

    // Initialize a random number generator through the program
    val rand = new Random(seed = 647)

    // Get mapping for BKV (Block-Key-Value) to reducer IDs (List of Ints)
    // Broadcast to all workers
    val k = args.reducers()
    val hmBKV2RID = sc.broadcast(Setup.setup(log, inputFile, k, rand))
    log.info("\nhmBKV2RID:\n" + hmBKV2RID.value.mkString("\n"))

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
    // Note that we sort the value within each partition based on the BKV, so that
    // Tuples with the same BKV are grouped together
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
