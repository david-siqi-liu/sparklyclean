package com.davidsiqiliu.sparklyclean.impl

import org.apache.spark.Partitioner

class DisDedupPartitioner(numReducers: Int) extends Partitioner {
  def numPartitions: Int = numReducers

  def getPartition(ridbkv: Any): Int = (ridbkv.asInstanceOf[(Int, String)]._1 - 1).hashCode % numPartitions
}
