package com.davidsiqiliu

import org.apache.spark.Partitioner

class DisDedupPartitioner(numReducers: Int) extends Partitioner {
  def numPartitions: Int = numReducers

  def getPartition(rid: Any): Int = rid.hashCode % numPartitions
}
