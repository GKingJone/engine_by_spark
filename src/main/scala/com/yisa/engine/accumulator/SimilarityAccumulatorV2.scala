package com.yisa.engine.accumulator

import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory
import scala.collection.immutable.TreeMap

/**
 * @author liliwei
 * @version 1.0
 * @since 2017年3月8日11:09:14
 */
class SimilarityAccumulatorV2 extends AccumulatorV2[String, TreeMap[Long, String]] {

  private val log = LoggerFactory.getLogger("SimilarityAccumulatorV2")

  private var result: TreeMap[Long, String] = new TreeMap[Long, String]()

  override def isZero: Boolean = {
    true
  }

  override def copy(): AccumulatorV2[String, TreeMap[Long, String]] = {
    val myAccumulator = new SimilarityAccumulatorV2()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = result.empty
  }

  override def add(v2: String): Unit = {
    val v1 = result

    var similarity = v2.split("_")(0)
    var platenumber = v2.split("_")(1)
    var solrid = v2.split("_")(2)
    val newresult = v1.+(similarity.toLong -> (platenumber + "_" + solrid))
    result = newresult

  }

  override def merge(other: AccumulatorV2[String, TreeMap[Long, String]]) = other match {
    case map: SimilarityAccumulatorV2 => {

      val v2 = other.value
      import scala.util.control.Breaks._
      breakable {
        var keyNo = 0
        v2.keys.foreach { key =>
          {
            result = result.+(key -> v2(key))
            keyNo += 1
          }
        } 
        if (keyNo > 100)
          break;
      }
    }

    case _ => 
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  } 

  override def value: TreeMap[Long, String] = {
    result
  }

}