/**
 * Customized comparison functions for each column
 */

package com.davidsiqiliu.sparklyclean.impl

import com.davidsiqiliu.sparklyclean.impl.Util._
import com.github.vickumar1981.stringdistance.StringDistance._

import scala.collection.mutable.ArrayBuffer

object Compare {

  def getFeatures(tuple1: String, tuple2: String): List[String] = {
    val t1 = tokenize(tuple1)
    val t2 = tokenize(tuple2)
    val features: ArrayBuffer[Double] = ArrayBuffer()

    /*
      0: rec_id (String) - ignore
      1: given_name (String)
      2: surname (String)
      3: street_number (Long)
      4: address_1 (String)
      5: address_2 (String)
      6: suburb (String)
      7: postcode (String)
      8: state (String)
      9: date_of_birth (Long)
      10: age (Long)
      11: phone_number (String)
      12: soc_sec_id (Long)
      13: blocking_number (Long) - ignore
     */
    val fields: List[(Int, String)] =
      List((0, "Ignore"), (1, "Levenshtein"), (2, "Levenshtein"), (3, "Levenshtein"), (4, "Levenshtein"), (5, "Levenshtein"), (6, "Levenshtein"),
        (7, "Levenshtein"), (8, "Levenshtein"), (9, "SqrtDiff"), (10, "AbsDiff"), (11, "Levenshtein"), (12, "Levenshtein"), (13, "Ignore"))

    // Customize comparison functions
    // Must output Doubles
    fields.map {
      case (idx, func) =>
        // Edit distance
        if (func == "Levenshtein") {
          features += Levenshtein.score(t1(idx).trim(), t2(idx).trim())
        }
        else if (func == "SqrtDiff") {
          try {
            features += math.sqrt(math.abs(t1(idx).trim().toLong - t2(idx).trim().toLong))
          } catch {
            case _: NumberFormatException =>
              // Both are missing values
              if (t1(idx).trim() == "" && t2(idx).trim() == "") {
                features += 1.0
              }
              // Only one is missing value
              else {
                features += Double.MaxValue
              }
          }
        } else if (func == "AbsDiff") {
          try {
            features += math.abs(t1(idx).trim().toLong - t2(idx).trim().toLong)
          } catch {
            case _: NumberFormatException =>
              if (t1(idx).trim() == "" && t2(idx).trim() == "") {
                features += 1.0
              } else {
                features += Double.MaxValue
              }
          }
        }
        // Ignored fields, e.g., rec_id, blocking_number
        // Add 0.0 as a placeholder for consistency purpose, does not affect prediction
        else {
          features += 0.0
        }
    }

    features.map(_.toString).toList
  }

  def compareWithinBlock(label: Boolean, bkv: BKV, leftTuples: ArrayBuffer[String], selfTuples: ArrayBuffer[String], rightTuples: ArrayBuffer[String]):
  List[String] = {
    val labeledPoints: ArrayBuffer[String] = ArrayBuffer()

    // Conduct L and R comparisons
    if (leftTuples.nonEmpty && rightTuples.nonEmpty) {
      for (i <- leftTuples.indices; j <- rightTuples.indices) {
        val t1 = leftTuples(i)
        val t2 = rightTuples(j)
        // Skip comparison if tuple pair has a block key in common that's lower than current block key
        // Otherwise, conduct comparison
        if (bkv.k <= lowestCommonBlockNum(t1, t2)) {
          // "t1Id, t2Id, label (if available), feature1, feature2, ..."
          labeledPoints += (Array(getId(t1), getId(t2), getLabel(label, t1, t2)) ++ getFeatures(t1, t2)).mkString(",")
        }
      }
    }
    // Conduct S comparisons
    else {
      for (i <- selfTuples.indices; j <- selfTuples.indices) {
        if (i < j) {
          val t1 = selfTuples(i)
          val t2 = selfTuples(j)
          if (bkv.k <= lowestCommonBlockNum(t1, t2)) {
            labeledPoints += (Array(getId(t1), getId(t2), getLabel(label, t1, t2)) ++ getFeatures(t1, t2)).mkString(",")
          }
        }
      }
    }

    labeledPoints.toList
  }
}
