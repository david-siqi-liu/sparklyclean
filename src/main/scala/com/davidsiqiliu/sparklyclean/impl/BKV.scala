package com.davidsiqiliu.sparklyclean.impl

case class BKV(k: Int, v: String) extends Ordered[BKV] {

  override def compare(that: BKV): Int =
    that match {
      case that: BKV =>
        this.k.compareTo(that.k) match {
          case 0 => this.v.compareTo(that.v)
          case r => r
        }
    }
}
