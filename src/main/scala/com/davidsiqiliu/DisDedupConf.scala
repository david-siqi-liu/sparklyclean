package com.davidsiqiliu

import org.rogach.scallop.{ScallopConf, ScallopOption}

class DisDedupConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = false, default = Some(""))
  val header: ScallopOption[Boolean] = opt[Boolean]()
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}
