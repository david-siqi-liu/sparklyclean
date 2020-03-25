/**
 * Map phase, return a list of ((reducer ID, BKV), ("S"/"L"/"R", tuple))
 */

package com.davidsiqiliu.sparklyclean.impl

import com.davidsiqiliu.sparklyclean.impl.Util._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DisDedupMapper {
  def map(tuple: String, bkv: BKV, rids: List[Int], k: Int, rand: Random):
  List[((Int, BKV), (String, String))] = {
    val k_i = rids.size
    // Single-reducer block
    if (k_i == 1) {
      List[((Int, BKV), (String, String))](((rids.head, bkv), ("S", tuple)))
    } else {
      // Multi-reducer block, triangle distribution strategy
      // Side length of the triangle
      val l_i = getL(k_i)
      // Generate anchor from [1, l_i]
      val a = rand.nextInt(l_i) + 1
      // Initialize output array
      val pairs: ArrayBuffer[((Int, BKV), (String, String))] = ArrayBuffer()
      var ridIndex = 0
      var rid = 0

      // LEFT
      for (p <- 1 until a) {
        ridIndex = (2 * l_i - p + 2) * (p - 1) / 2 + (a - p)
        rid = rids(ridIndex)
        pairs += (((rid, bkv), ("L", tuple)))
      }

      // SELF
      ridIndex = (2 * l_i - a + 2) * (a - 1) / 2
      rid = rids(ridIndex)
      pairs += (((rid, bkv), ("S", tuple)))

      // RIGHT
      for (q <- a + 1 to l_i) {
        ridIndex = (2 * l_i - a + 2) * (a - 1) / 2 + (q - a)
        rid = rids(ridIndex)
        pairs += (((rid, bkv), ("R", tuple)))
      }

      pairs.toList
    }
  }
}
