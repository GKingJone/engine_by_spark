package com.yisa.engine.accumulator

import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory
import scala.collection.immutable.TreeMap

class MultiPointAccumulatorV2 extends AccumulatorV2[String, TreeMap[String, String]] {

  private val log = LoggerFactory.getLogger("MultiPointAccumulatorV2")

  private var result: TreeMap[String, String] = new TreeMap[String, String]()

  // 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
  override def isZero: Boolean = {
    true
  }

  // 拷贝一个新的AccumulatorV2
  override def copy(): AccumulatorV2[String, TreeMap[String, String]] = {
    val myAccumulator = new MultiPointAccumulatorV2()
    myAccumulator.result = this.result
    myAccumulator
  }

  //  重置AccumulatorV2中的数据
  override def reset(): Unit = {
    result = result.empty

  }

  // 操作数据累加方法实现 
  // key : platenumber_taskid
  // values : solrid_levelid_brandid_modelid_yearid_direction_colorid_capturetime
  override def add(v: String): Unit = {
    
    // yearid0_platenumber1_direction2_colorid3_modelid4_brandid5_levelid6_lastcaptured7_issheltered8_solrid9_taskId10_capturetime11
   
    val yearid = v.split("_")(0)
    val platenumber = v.split("_")(1)
    val direction = v.split("_")(2)
    val colorid = v.split("_")(3)
    val modelid = v.split("_")(4)
    val brandid = v.split("_")(5)
    val levelid = v.split("_")(6)
    val taskid = v.split("_")(10)
    
    val solrid =v.split("_")(9)
    val capturetime = v.split("_")(11)
    
    val key = platenumber + "_" + taskid
    val value = solrid + "_" + levelid + "_" + brandid+ "_" + modelid+ "_" + yearid+ "_" + direction+ "_" + colorid+ "_" + capturetime
    if (result.contains(key)) {
      val value2 = result(key)
      val capOld = value2.split("_")(7)
      if(capOld.toString().toInt < capturetime.toString().toInt){
        val newresult = result.+(key -> value)
        result = newresult
      }
    } else {
      val newresult = result.+(key -> value)
      result = newresult
    }

  }

  // 合并数据 
  override def merge(other: AccumulatorV2[String, TreeMap[String, String]]) = other match {
    case map: MultiPointAccumulatorV2 => {

      val v2 = other.value
      v2.keys.foreach { key =>
        {
          val value = v2(key)
          if (result.contains(key)) {
            val value2 = result(key)
            val capOld = value2.split("_")(7)
            val capNew = value.split("_")(7)
            if(capOld.toString().toInt < capNew.toString().toInt){
              val newresult = result.+(key -> value)
              result = newresult
            }
          } else {
            val newresult = result.+(key -> value)
            result = newresult
          }
        }
      }
    }

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  // AccumulatorV2对外访问的数据结果
  override def value: TreeMap[String, String] = {
    result
  }

}