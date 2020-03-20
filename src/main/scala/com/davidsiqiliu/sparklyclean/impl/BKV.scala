package com.davidsiqiliu.sparklyclean.impl

case class BKV(b: Int, v: String) extends Ordered[BKV] {

  def canEqual(a: Any): Boolean = a.isInstanceOf[BKV]

  override def equals(that: Any): Boolean =
    that match {
      case that: BKV => {
        that.canEqual(this) &&
          this.b == that.b &&
          this.v == that.v
      }
      case _ => false
    }

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
