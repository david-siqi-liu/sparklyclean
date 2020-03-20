package com.davidsiqiliu.sparklyclean.impl

import scala.collection.mutable.ArrayBuffer

object DisDedupReducer {
  def reduce(iter: Iterator[((Int, BKV), (String, String))]):
  Iterator[(Double, (String, String))] = {
    var leftTuples: ArrayBuffer[String] = ArrayBuffer()
    var selfTuples: ArrayBuffer[String] = ArrayBuffer()
    var rightTuples: ArrayBuffer[String] = ArrayBuffer()
    val similarities: ArrayBuffer[(Double, (String, String))] = ArrayBuffer()

    var pair = ((0, BKV(0, "")), ("", "")) // ((rid, bkv), (L/S/R, tuple))
    var bkvPrev = BKV(0, "")
    var bkvCurr = BKV(0, "")
    var side = ""
    var tuple = ""
    while (iter.hasNext) {
      pair = iter.next
      // Parse
      bkvCurr = pair._1._2
      side = pair._2._1
      tuple = pair._2._2
      // Same block
      if (bkvCurr == bkvPrev) {
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
        similarities ++= Util.compareWithinBlock(bkvPrev, leftTuples, selfTuples, rightTuples)
        // Reset tuple lists
        leftTuples.clear()
        rightTuples.clear()
        selfTuples.clear()
        // Add into tuple list
        if (side == "L") {
          leftTuples += tuple
        } else if (side == "R") {
          rightTuples += tuple
        } else {
          selfTuples += tuple
        }
        // Set bkvPrev to bkvCurr
        bkvPrev = bkvCurr
      }
    }

    // Conduct comparison for the last block
    similarities ++= Util.compareWithinBlock(bkvPrev, leftTuples, selfTuples, rightTuples)

    similarities.iterator
  }
}
