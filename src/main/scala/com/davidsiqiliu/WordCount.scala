package com.davidsiqiliu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("WordCount").setMaster("local[2]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val data: RDD[String] = sparkContext.textFile("./data/test.txt")
    val words: RDD[String] = data.flatMap(x => x.split(" "))
    val wordAndOne: RDD[(String, Int)] = words.map(x => (x, 1))
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x+y)
    result.foreach(println(_))
    sparkContext.stop()
  }
}