package com.davidsiqiliu.sparklyclean.impl

import scala.collection.mutable.ArrayBuffer

object Util {
  def getBKVs(tuple: String): List[String] = {
    val numBlocks = 2
    val STATE_POS = 8
    val BKV_POS = 13

    val t = tuple.split(",")
    val bkvs: ArrayBuffer[String] = new ArrayBuffer()
    //    for (j <- 1 to numBlocks){
    //        ...
    //    }
    bkvs += "h1#" + t(STATE_POS).trim()
    bkvs += "h2#" + t(BKV_POS).trim()

    bkvs.toList
  }

  def getL(k_i: Int): Int = {
    val l_i = math.floor(math.sqrt(2 * k_i)).toInt
    if (l_i * (l_i + 1) / 2 <= k_i) {
      l_i
    } else {
      l_i - 1
    }
  }

  def computeSimilarity(t1: Array[String], t2: Array[String]): Double = {
    if (t1(0).split("-")(1) == t2(0).split("-")(1)) {
      1.0
    } else {
      0.001
    }
  }

  def compareWithinBlock(leftTuples: ArrayBuffer[String], selfTuples: ArrayBuffer[String], rightTuples: ArrayBuffer[String]):
  ArrayBuffer[(Double, (String, String))] = {
    val similarities: ArrayBuffer[(Double, (String, String))] = ArrayBuffer()

    if (leftTuples.nonEmpty && rightTuples.nonEmpty) {
      for (i <- leftTuples.indices; j <- rightTuples.indices) {
        val t1 = leftTuples(i).split(",")
        val t2 = rightTuples(j).split(",")
        similarities += ((Util.computeSimilarity(t1, t2), (t1(0), t2(0))))
      }
    } else {
      for (i <- selfTuples.indices; j <- selfTuples.indices) {
        if (i < j) {
          val t1 = selfTuples(i).split(",")
          val t2 = selfTuples(j).split(",")
          similarities += ((Util.computeSimilarity(t1, t2), (t1(0), t2(0))))
        }
      }
    }

    similarities
  }
}
