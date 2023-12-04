package com.yisa.engine.test

import org.apache.spark.sql.SparkSession
import com.yisa.engine.accumulator.SimilarityAccumulatorV2
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.TreeMap
import org.apache.spark.util.AccumulatorV2

object SparkLocal {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("SparkEngine")
      .master("local[2]")
      .getOrCreate()
      
      
       val indexs2 = ArrayBuffer[Int](1,2,3,3)
    val distIndexs2 = sparkSession.sparkContext.parallelize(indexs2)
 val  indexs3 =   distIndexs2.flatMap(x=>x.to(3))
 
 

      
      indexs3.foreach(println)

//    val accumulator = new SimilarityAccumulatorV2()
//    sparkSession.sparkContext.register(accumulator, "SimilarityAccumulator")
////    println(accumulator.isRegistered)
//
//    val indexs = ArrayBuffer[String]()
//    for (dateid <- 1 to 4) {
//      indexs += (dateid + "")
//    }
//    println("indexs:" + indexs)
//    val distIndexs = sparkSession.sparkContext.parallelize(indexs)
//    distIndexs.foreach { index =>
//      {
//        println("index:" + index)
//        accumulator.add(index + "_" + "nnnn" + "_" + "solrid")
//      }
//    }
//    println("")
//
//    println(accumulator.value)
//
//    var treeMap: TreeMap[Long, String] = accumulator.value
//
//    treeMap.keys.foreach { key => println(key) }
//    println("treeMap size:" + treeMap.size)
  }

}