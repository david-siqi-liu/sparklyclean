package com.davidsiqiliu

import org.rogach.scallop.{ScallopConf, ScallopOption}

class DisDedupConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = false, default = Some(""))
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold: ScallopOption[Double] = opt[Double](descr = "similarity threshold", required = false, default = Some(0.0))
  val header: ScallopOption[Boolean] = opt[Boolean]()
  val coalesce: ScallopOption[Boolean] = opt[Boolean]()
  verify()
}
