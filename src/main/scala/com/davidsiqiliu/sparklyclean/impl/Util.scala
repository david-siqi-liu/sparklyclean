package com.davidsiqiliu.sparklyclean.impl

import scala.collection.mutable.ArrayBuffer

object Util {
  def tokenize(tuple: String): Array[String] = {
    val t = tuple.split(",")

    t
  }

  def getId(tuple: String): String = {
    val t = tokenize(tuple)

    t(0).trim()
  }

  def getBKVs(tuple: String): List[BKV] = {
    val numBlocks = 2
    val STATE_POS = 8
    val BKV_POS = 13

    val t = tokenize(tuple)
    val bkvs: ArrayBuffer[BKV] = new ArrayBuffer()
    //    for (j <- 1 to numBlocks){
    //        ...
    //    }
    bkvs += BKV(1, Option(t(BKV_POS).trim()).getOrElse(""))
    bkvs += BKV(2, Option(t(STATE_POS).trim()).getOrElse(""))

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

  def computeSimilarity(tuple1: String, tuple2: String): Double = {
    /*
     TO-DO
     */
    val t1ID = getId(tuple1)
    val t2ID = getId(tuple2)

    if (t1ID.split("-")(1) == t2ID.split("-")(1)) {
      1.0
    } else {
      0.001
    }
  }

  def lowestCommonBlockNum(tuple1: String, tuple2: String): Int = {
    val t1BKVs: List[BKV] = getBKVs(tuple1)
    val t2BKVs: List[BKV] = getBKVs(tuple2)

    var lowest: Int = Int.MaxValue

    for (t1bkv <- t1BKVs; t2bkv <- t2BKVs){
      if (t1bkv == t2bkv) {
        lowest = math.min(lowest, t1bkv.b)
      }
    }

    lowest
  }

  def compareWithinBlock(bkv: BKV, leftTuples: ArrayBuffer[String], selfTuples: ArrayBuffer[String], rightTuples: ArrayBuffer[String]):
  ArrayBuffer[(Double, (String, String))] = {
    val similarities: ArrayBuffer[(Double, (String, String))] = ArrayBuffer()

    if (leftTuples.nonEmpty && rightTuples.nonEmpty) {
      for (i <- leftTuples.indices; j <- rightTuples.indices) {
        val t1 = leftTuples(i)
        val t2 = rightTuples(j)
        if (bkv.b <= lowestCommonBlockNum(t1, t2)) {
          similarities += ((computeSimilarity(t1, t2), (getId(t1), getId(t2))))
        }
      }
    } else {
      for (i <- selfTuples.indices; j <- selfTuples.indices) {
        if (i < j) {
          val t1 = selfTuples(i)
          val t2 = selfTuples(j)
          if (bkv.b <= lowestCommonBlockNum(t1, t2)) {
            similarities += ((computeSimilarity(t1, t2), (getId(t1), getId(t2))))
          }
        }
      }
    }

    similarities
  }
}
