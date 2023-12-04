package com.yisa.engine.accumulator

import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory
import com.yisa.engine.uitl.StringUtils
import scala.collection.immutable.HashSet
import scala.collection.immutable.HashMap

/**
 * @author liliwei
 * @version 1.0
 * @since 2017年3月8日11:09:14
 */
class FrequentlyCarAccumulatorV2 extends AccumulatorV2[String, HashMap[String, HashSet[String]]] {

  private val log = LoggerFactory.getLogger("FrequentlyCarAccumulatorV2")

  private var result: HashMap[String, HashSet[String]] = new HashMap[String, HashSet[String]]()

  override def isZero: Boolean = {
    true
  }

  override def copy(): AccumulatorV2[String, HashMap[String, HashSet[String]]] = {
    val myAccumulator = new FrequentlyCarAccumulatorV2()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = result.empty
  }

  override def add(v2: String): Unit = {
    val v1 = result
    val locationid = v2.split("_")(0)
    val platenumber = v2.split("_")(1)
    val solrid = v2.split("_")(2)

    val key = locationid + "_" + platenumber
    if (v1.contains(key)) {
      var value = v1(key)
      value = value.+(solrid)
      val newresult = result.+(key -> value)
      result = newresult
    } else {
      val value = HashSet[String](solrid)
      val newresult = v1.+(key -> value)
      result = newresult
    }
  }

  override def merge(other: AccumulatorV2[String, HashMap[String, HashSet[String]]]) = other match {
    case map: FrequentlyCarAccumulatorV2 => {

      val v2 = other.value
      v2.keys.foreach { key =>
        {
          val value_other = v2(key)

          if (result.contains(key)) {
            var value_pre = result(key)
            value_other.foreach(v => {
              value_pre = value_pre.+(v)
            })
            val new_result = result.+(key -> value_pre)
            result = new_result
          } else {
            val new_result = result.+(key -> value_other)
            result = new_result
          }
        }
      }
    }

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: HashMap[String, HashSet[String]] = {
    result
  }

}