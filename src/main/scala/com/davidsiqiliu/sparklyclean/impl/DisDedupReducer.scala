/**
 * Reduce phase
 *  1. Group tuples with the same BKV together
 *  2. Conduct comparisons within each group
 *  3. Return an Iterator of "t1Id, t2Id, label (if available), feature1, feature2, ..."
 */

package com.davidsiqiliu.sparklyclean.impl

import scala.collection.mutable.ArrayBuffer

object DisDedupReducer {
  def reduce(label: Boolean, iter: Iterator[((Int, BKV), (String, String))]):
  Iterator[String] = {
    var leftTuples: ArrayBuffer[String] = ArrayBuffer()
    var selfTuples: ArrayBuffer[String] = ArrayBuffer()
    var rightTuples: ArrayBuffer[String] = ArrayBuffer()
    val labeledPoints: ArrayBuffer[String] = ArrayBuffer()

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
        labeledPoints ++= Compare.compareWithinBlock(label, bkvPrev, leftTuples, selfTuples, rightTuples)
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
    labeledPoints ++= Compare.compareWithinBlock(label, bkvPrev, leftTuples, selfTuples, rightTuples)

    labeledPoints.iterator
  }
}
