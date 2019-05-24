package com.MLToolsTest.otherALS

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @author zhaoming on 2017-12-01 13:46
  **/
class MyUDAF extends  UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = StructType(Array(StructField("input",StringType,true)))

  //聚合时所要处理的数据的结果类型
  override def bufferSchema: StructType = StructType(Array(StructField("count",IntegerType ,true)))

  //uDAF计算后饭后的类型
  override def dataType: DataType = IntegerType

  //保持一致性
  override def deterministic: Boolean = true

  //在aggregate 之前每组数据的初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0) = 0}

  //聚合时，计算结果
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0)+1
  }

  //在分布式节点上进行Local Reduce
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }

  //返回计算结果
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}













