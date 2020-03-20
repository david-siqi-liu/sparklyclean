package com.davidsiqiliu.sparklyclean.impl

case class BKV(b: Int, v: String) extends Ordered[BKV] {

  override def compare(that: BKV): Int =
    that match {
      case that: BKV => {
        this.b.compareTo(that.b) match {
          case 0 => this.v.compareTo(that.v)
          case r => r
        }
      }
    }
}
