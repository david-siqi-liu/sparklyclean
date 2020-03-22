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
        (7, "Levenshtein"), (8, "Levenshtein"), (9, "SqrtDiff"), (10, "SqrtDiff"), (11, "SqrtDiff"), (12, "SqrtDiff"), (13, "Ignore"))

    fields.map {
      case (idx, func) =>
        if (func == "Levenshtein") {
          features += Levenshtein.score(t1(idx), t2(idx))
        }
        else if (func == "SqrtDiff") {
          try {
            features += math.sqrt(math.abs(t1(idx).toInt - t2(idx).toInt))
          } catch {
            case _: NumberFormatException => features += Double.MaxValue
          }
        } else {
          features += 0.0
        }
    }

    features.map(_.toString).toList
  }

  def compareWithinBlock(label: Boolean, bkv: BKV, leftTuples: ArrayBuffer[String], selfTuples: ArrayBuffer[String], rightTuples: ArrayBuffer[String]):
  List[String] = {
    // [t1Id, t2Id, label (if available), feature1, feature2, ...]
    val labeledPoints: ArrayBuffer[String] = ArrayBuffer()

    if (leftTuples.nonEmpty && rightTuples.nonEmpty) {
      for (i <- leftTuples.indices; j <- rightTuples.indices) {
        val t1 = leftTuples(i)
        val t2 = rightTuples(j)
        if (bkv.b <= lowestCommonBlockNum(t1, t2)) {
          labeledPoints += (Array(getId(t1), getId(t2), getLabel(label, t1, t2)) ++ getFeatures(t1, t2)).mkString(",")
        }
      }
    } else {
      for (i <- selfTuples.indices; j <- selfTuples.indices) {
        if (i < j) {
          val t1 = selfTuples(i)
          val t2 = selfTuples(j)
          if (bkv.b <= lowestCommonBlockNum(t1, t2)) {
            labeledPoints += (Array(getId(t1), getId(t2), getLabel(label, t1, t2)) ++ getFeatures(t1, t2)).mkString(",")
          }
        }
      }
    }

    labeledPoints.toList
  }
}
