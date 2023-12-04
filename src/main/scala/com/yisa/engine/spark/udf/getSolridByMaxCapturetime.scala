package com.yisa.engine.spark.udf

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

/**
 * @date  2016年12月8日
 * 得到最大capturetime对应的Solrid
 */
class getSolridByMaxCapturetime extends UserDefinedAggregateFunction {
  
  // 输入参数的数据类型定义
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("solr", StringType) :: StructField("capturetime", LongType) :: Nil)

  // 聚合的中间过程中产生的数据的数据类型定义
  override def bufferSchema: StructType = StructType(
    StructField("max_solr", StringType) :: StructField("max_capturetime", LongType) :: Nil)

  //聚合结果的数据类型定义
  override def dataType: DataType = StringType

  //一致性检验，如果为true,那么输入不变的情况下计算的结果也是不变的。 
  override def deterministic: Boolean = true

  //设置聚合中间buffer的初始值，但需要保证这个语义：两个初始buffer调用下面实现的merge方法后也应该为初始buffer。  
   //即如果你初始值是1，然后你merge是执行一个相加的动作，两个初始buffer合并之后等于2，不会等于初始buffer了。这样的初始值就是有问题的，所以初始值也叫"zero value
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = "max_solr"
    buffer(1) = 0l
  }

  //用输入数据input更新buffer值,类似于combineByKey
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    
        
    if (input.getAs[Long](1) > buffer.getAs[Long](1)) {
        buffer.update(0 , input.getAs[String](0))
        buffer.update(1 , input.getAs[String](1))
    }

  }

  //合并两个buffer,将buffer2合并到buffer1.在合并两个分区聚合结果的时候会被用到,类似于reduceByKey  
   //这里要注意该方法没有返回值，在实现的时候是把buffer2合并到buffer1中去，你需要实现这个合并细节
  override def merge(buffer1: MutableAggregationBuffer, input: Row): Unit = {
    
        
    if (input.getAs[Long](1) > buffer1.getAs[Long](1)) {
        buffer1.update(0 , input.getAs[String](0))
        buffer1.update(1 , input.getAs[String](1))
    }
  }

  //计算并返回最终的聚合结果 
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }
}  