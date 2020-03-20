package com.davidsiqiliu.sparklyclean.impl

import com.davidsiqiliu.sparklyclean.impl.Util._
import com.github.vickumar1981.stringdistance.StringDistance._

import scala.collection.mutable.ArrayBuffer

object Compare {

  def compare(tuple1: String, tuple2: String): List[Double] = {
    val t1 = tokenize(tuple1)
    val t2 = tokenize(tuple2)
    val vector: ArrayBuffer[Double] = ArrayBuffer()

    /*
      0: rec_id (String) - ignore
      1: given_name (String)
      2: surname (String)
      3: street_number (Int)
      4: address_1 (String)
      5: address_2 (String)
      6: suburb (String)
      7: postcode (String)
      8: state (String)
      9: date_of_birth (Int)
      10: age (Int)
      11: phone_number (String)
      12: soc_sec_id (Int)
      13: blocking_number (Int) - ignore
     */
    val fields: List[(Int, String)] =
      List((0, "Ignore"), (1, "Levenshtein"), (2, "Levenshtein"), (3, "Levenshtein"), (4, "Levenshtein"), (5, "Levenshtein"), (6, "Levenshtein"),
        (7, "Levenshtein"), (8, "Levenshtein"), (9, "SqrtDiff"), (10, "SqrtDiff"), (11, "SqrtDiff"), (12, "SqrtDiff"), (13, "Ignore"))

    fields.map {
      case (idx, func) =>
        if (func == "Levenshtein") {
          vector += Levenshtein.score(t1(idx), t2(idx))
        }
        else if (func == "SqrtDiff") {
          try {
            vector += math.sqrt(math.abs(t1(idx).toInt - t2(idx).toInt))
          } catch {
            case _: NumberFormatException => vector += -1.0
          }
        } else {
          vector += 0.0
        }
    }

    vector.toList
  }

  def compareWithinBlock(bkv: BKV, leftTuples: ArrayBuffer[String], selfTuples: ArrayBuffer[String], rightTuples: ArrayBuffer[String]):
  ArrayBuffer[((String, String), List[Double])] = {
    val comparisonVectors: ArrayBuffer[((String, String), List[Double])] = ArrayBuffer()

    if (leftTuples.nonEmpty && rightTuples.nonEmpty) {
      for (i <- leftTuples.indices; j <- rightTuples.indices) {
        val t1 = leftTuples(i)
        val t2 = rightTuples(j)
        if (bkv.b <= lowestCommonBlockNum(t1, t2)) {
          comparisonVectors += (((getId(t1), getId(t2)), compare(t1, t2)))
        }
      }
    } else {
      for (i <- selfTuples.indices; j <- selfTuples.indices) {
        if (i < j) {
          val t1 = selfTuples(i)
          val t2 = selfTuples(j)
          if (bkv.b <= lowestCommonBlockNum(t1, t2)) {
            comparisonVectors += (((getId(t1), getId(t2)), compare(t1, t2)))
          }
        }
      }
    }

    comparisonVectors
  }
}
