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

  def getLabel(label: Boolean, tuple1: String, tuple2: String): String = {
    // Know the ground truth
    if (label) {
      val t1Id = getId(tuple1)
      val t2Id = getId(tuple2)

      // Duplicate
      if (t1Id.split("-")(1)== t2Id.split("-")(1)){
        "1"
      } else {
        "0"
      }
    } else {
      ""
    }
  }

  def getBKVs(tuple: String): List[BKV] = {
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

  def lowestCommonBlockNum(tuple1: String, tuple2: String): Long = {
    val t1BKVs: List[BKV] = getBKVs(tuple1)
    val t2BKVs: List[BKV] = getBKVs(tuple2)

    var lowest: Long = Int.MaxValue

    for (t1bkv <- t1BKVs; t2bkv <- t2BKVs) {
      if (t1bkv == t2bkv) {
        lowest = math.min(lowest, t1bkv.b)
      }
    }

    lowest
  }

}
