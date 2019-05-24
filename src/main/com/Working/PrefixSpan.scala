package com.Working

import com.Common.GetConf
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.sql.functions._

/**
  * @author zhaoming on 2018-08-02 11:01
  **/
object PrefixSpan {

  def main(args: Array[String]): Unit = {

    val getConf = new GetConf()
    val sc = getConf.getConfSingle
    val sequences = sc.parallelize(Seq(
      Array(Array(1, 2), Array(3)),
      Array(Array(1), Array(3, 2), Array(1, 2)),
      Array(Array(1, 2), Array(5)),
      Array(Array(6))
    ), 2).cache()
    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
    val model = prefixSpan.run(sequences)
    model.freqSequences.collect().foreach { freqSequence =>
      println(
        freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          ", " + freqSequence.freq)
    }
  }

}
