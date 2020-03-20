package com.davidsiqiliu.sparklyclean.impl

import scala.collection.mutable.ArrayBuffer

object DisDedupReducer {
  def reduce(iter: Iterator[((Int, String), (String, String))]):
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
        similarities ++= Util.compareWithinBlock(leftTuples, selfTuples, rightTuples)
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
        // Set prevbkv to bkv
        prevbkv = bkv
      }
    }

    // Conduct comparison for the last block
    similarities ++= Util.compareWithinBlock(leftTuples, selfTuples, rightTuples)

    similarities.iterator
  }
}
