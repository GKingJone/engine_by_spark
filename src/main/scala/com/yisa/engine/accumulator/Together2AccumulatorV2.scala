package com.yisa.engine.accumulator

import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

/**
 * @author liliwei
 * @version 1.0
 * @since 2017年3月8日11:09:14
 */
class Together2AccumulatorV2 extends AccumulatorV2[String, TreeMap[String, Map[String, ArrayBuffer[String]]]] {

  private val log = LoggerFactory.getLogger("Together2AccumulatorV2")

  private var result: TreeMap[String, Map[String, ArrayBuffer[String]]] = new TreeMap[String, Map[String, ArrayBuffer[String]]]()

  override def isZero: Boolean = {
    true
  }

  override def copy(): AccumulatorV2[String, TreeMap[String, Map[String, ArrayBuffer[String]]]] = {
    val myAccumulator = new Together2AccumulatorV2()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = result.empty
  }

  override def add(v2: String): Unit = {
    val v1 = result
    //platenumber_solrid_locationid_issheltered_lastcaptured_yearid_dateid

    var platenumber = v2.split("_")(0)
    var solrid = v2.split("_")(1)
    var locationid = v2.split("_")(2)
    var issheltered = v2.split("_")(3)
    var lastcaptured = v2.split("_")(4)
    var yearid = v2.split("_")(5)
    var dateid = v2.split("_")(6)

    if (v1.contains(platenumber)) {

      val _value = v1.get(platenumber).get

      var platenumber_ = _value.get("platenumber").get.+=(platenumber)
      var solrid_ = _value.get("solrid").get.+=(solrid)
      var locationid_ = _value.get("locationid").get.+=(locationid)
      var issheltered_ = _value.get("issheltered").get.+=(issheltered)
      var lastcaptured_ = _value.get("lastcaptured").get.+=(lastcaptured)
      var yearid_ = _value.get("yearid").get.+=(yearid)
      var dateid_ = _value.get("dateid").get.+=(dateid)

      val value_1: Map[String, ArrayBuffer[String]] = Map(
        "platenumber" -> platenumber_,
        "solrid" -> solrid_,
        "locationid" -> locationid_,
        "issheltered" -> issheltered_,
        "lastcaptured" -> lastcaptured_,
        "yearid" -> yearid_,
        "dateid" -> dateid_)
      val newresult = v1.+(platenumber -> value_1)
      result = newresult

    } else {
      val value_1: Map[String, ArrayBuffer[String]] = Map(
        "platenumber" -> ArrayBuffer[String](platenumber),
        "solrid" -> ArrayBuffer[String](solrid),
        "locationid" -> ArrayBuffer[String](locationid),
        "issheltered" -> ArrayBuffer[String](issheltered),
        "lastcaptured" -> ArrayBuffer[String](lastcaptured),
        "yearid" -> ArrayBuffer[String](yearid),
        "dateid" -> ArrayBuffer[String](dateid))
      val newresult = v1.+(platenumber -> value_1)
      result = newresult
    }

  }

  override def merge(other: AccumulatorV2[String, TreeMap[String, Map[String, ArrayBuffer[String]]]]) = other match {
    case map: Together2AccumulatorV2 => {

      val v2 = other.value
//      var v1 = result
      v2.keys.foreach { platenumber_key =>
        {

          if (result.contains(platenumber_key)) {

            val _value = v2.get(platenumber_key).get
            
            var platenumber_v2 = _value.get("platenumber").get
            var solrid_v2 = _value.get("solrid").get
            var locationid_v2 = _value.get("locationid").get
            var issheltered_v2 = _value.get("issheltered").get
            var lastcaptured_v2 = _value.get("lastcaptured").get
            var yearid_v2 = _value.get("yearid").get
            var dateid_v2 = _value.get("dateid").get

            val _value_v1 = result.get(platenumber_key).get
            var platenumber_ = _value_v1.get("platenumber").get.union(platenumber_v2)
            var solrid_ = _value_v1.get("solrid").get.union(solrid_v2)
            var locationid_ = _value_v1.get("locationid").get.union(locationid_v2)
            var issheltered_ = _value_v1.get("issheltered").get.union(issheltered_v2)
            var lastcaptured_ = _value_v1.get("lastcaptured").get.union(lastcaptured_v2)
            var yearid_ = _value_v1.get("yearid").get.union(yearid_v2)
            var dateid_ = _value_v1.get("dateid").get.union(dateid_v2)

            val value_1: Map[String, ArrayBuffer[String]] = Map(
              "platenumber" -> platenumber_,
              "solrid" -> solrid_,
              "locationid" -> locationid_,
              "issheltered" -> issheltered_,
              "lastcaptured" -> lastcaptured_,
              "yearid" -> yearid_,
              "dateid" -> dateid_)
            result = result.+(platenumber_key -> value_1)

//            v1 = result
          } else {

            val _value = v2.get(platenumber_key).get
            var platenumber_v2 = _value.get("platenumber").get
            var solrid_v2 = _value.get("solrid").get
            var locationid_v2 = _value.get("locationid").get
            var issheltered_v2 = _value.get("issheltered").get
            var lastcaptured_v2 = _value.get("lastcaptured").get
            var yearid_v2 = _value.get("yearid").get
            var dateid_v2 = _value.get("dateid").get

            val value_1: Map[String, ArrayBuffer[String]] = Map(
              "platenumber" -> platenumber_v2,
              "solrid" -> solrid_v2,
              "locationid" -> locationid_v2,
              "issheltered" -> issheltered_v2,
              "lastcaptured" -> lastcaptured_v2,
              "yearid" -> yearid_v2,
              "dateid" -> dateid_v2)
            result = result.+(platenumber_key -> value_1)
//            v1 = result
          }
        }
      }
    }

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: TreeMap[String, Map[String, ArrayBuffer[String]]] = {
    result
  }

}