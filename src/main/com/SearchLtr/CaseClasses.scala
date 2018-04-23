package com.SearchLtr

import org.apache.spark.mllib.linalg.Vector

import scala.beans.BeanInfo

/**
  * Created by wangyongxuan on 2017/8/1.
  */
case class TitleInfoStruct(id: String, synonymTitle: String)

case class TitleInfo(info: String)

@BeanInfo
case class IndexItem(qid: String, rank: Int, x: Vector, y: Int)
//case class IndexItem(qid: String, rank: Int, x: Vector, y: Double)

case class NDCG(k: Int) {
  def score(ranklist: RankList): Double = {
    var sum = 0d
    var count = 0
    for (i <- ranklist.ys.indices) {
      if (ranklist.ys(i) != 0) {
        sum += NDCG.discount(i)
        count += 1
      }
    }
    if (count > 0) sum / NDCG.idealDCG(count) else 0
  }
}

object NDCG {
  val maxSize = 2000
  val discount = {
    val LOG2 = math.log(2)
    val arr = Array.ofDim[Double](maxSize)
    for (i <- arr.indices) {
      arr(i) = LOG2 / math.log(2 + i)
    }
    arr
  }

  val idealDCG = {
    val arr = Array.ofDim[Double](maxSize + 1)
    arr(0) = 0
    for (i <- 1 until arr.length) {
      arr(i) = arr(i - 1) + discount(i - 1)
    }
    arr
  }
}

case class RankList(qid: String, ys: Array[Int]) {
  override def toString: String = {
    "%s: %s".format(qid, ys.mkString(","))
  }
}

case class Weight(key: String, weight: Double)