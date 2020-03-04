/**
 * Spark implementation of Distributed Data Deduplication (http://www.vldb.org/pvldb/vol9/p864-chu.pdf)
 */

package com.davidsiqiliu

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.rogach.scallop._

import scala.util.Random
import scala.collection.mutable.ArrayBuffer


class DisDedupConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = false, default = Some(""))
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}


object DisDedup {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {

    val args = new DisDedupConf(argv)

    val sparkConf = new SparkConf().setAppName("DisDedup")
    val sparkContext = new SparkContext(sparkConf)

    // Read in input file
    val inputFile = sparkContext.textFile(args.input())
    log.info("Input: " + args.input())

    // Size of triangle
    var l = math.floor(math.sqrt(2 * args.reducers())).toInt
    var k = l * (l + 1) / 2
    if (k > args.reducers()) {
      l = l - 1
      k = l * (l + 1) / 2
    }
    log.info("Number of reducers: " + k)

    // Partitioner
    class DisDedupPartitioner(numReducers: Int) extends Partitioner {
      def numPartitions: Int = numReducers

      def getPartition(rid: Any): Int = rid.hashCode % numPartitions
    }

    // Random integer generator
    val rand = new Random(seed = 1)

    // Anchor
    var a : Int = 0

    // Reducer ID
    var rid : Int = 0

    // Mapper
    val mapper = inputFile
      .flatMap(line => {
        // Tokenize
        val tokens = line.split(",")
        val blockNum = tokens(0).toInt
        val tuple = tokens(1)

        // Generate anchor
        a = rand.nextInt(l) + 1
        print("Anchor for " + tuple + " : " + a + "\n")

        // Output
        val pairs: ArrayBuffer[(Integer, String)] = ArrayBuffer()

        // LEFT
        for (p <- 1 until a) {
          rid = (2 * l - p + 2) * (p - 1) / 2 + (a - p)
          pairs += (rid, "L#" + tuple).asInstanceOf[(Integer, String)]
        }

        // SELF
        rid = (2 * l - a + 2) * (a - 1) / 2
        pairs += (rid, "S#" + tuple).asInstanceOf[(Integer, String)]

        // RIGHT
        for (q <- a + 1 to l) {
          rid = (2 * l - a + 2) * (a - 1) / 2 + (q - a)
          pairs += (rid, "R#" + tuple).asInstanceOf[(Integer, String)]
        }

        // Emit
        pairs.toList
      })

    // Reducer
    val reducer = mapper
      .repartitionAndSortWithinPartitions(new DisDedupPartitioner(k))

    reducer.foreach(println(_))

    if (args.output() != "") {
      FileSystem.get(sparkContext.hadoopConfiguration).delete(new Path(args.output()), true)
      reducer.saveAsTextFile(args.output())
    }

    sparkContext.stop()
  }
}