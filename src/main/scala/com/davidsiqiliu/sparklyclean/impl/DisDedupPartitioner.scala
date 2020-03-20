package com.davidsiqiliu.sparklyclean.impl

import org.apache.spark.Partitioner

class DisDedupPartitioner(numReducers: Int) extends Partitioner {
  def numPartitions: Int = numReducers

  def getPartition(ridbkv: Any): Int = {
    val rid: Int = ridbkv.asInstanceOf[(Int, BKV)]._1

    (rid - 1).hashCode % numPartitions
  }
}
