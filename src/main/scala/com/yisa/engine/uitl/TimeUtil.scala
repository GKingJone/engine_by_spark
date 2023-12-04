package com.yisa.engine.uitl

import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp
import java.util.Calendar

/**
 * @author liliwei
 * @date  2016年9月9日
 * time util
 */
object TimeUtil  extends Serializable {
  
   //决定系统缓存多少数据，以天为单位
  def getLCacheDataDateid(days: Int): String = {
    var cal = Calendar.getInstance();
    cal.setTime(new Date());
    cal.add(Calendar.DAY_OF_MONTH, -days);
    val format = new SimpleDateFormat("yyyyMMdd")
    format.format(cal.getTime())
  }
  
  def getDateId(): Int = {
    val format = new SimpleDateFormat("yyyyMMdd")
    var today = new Date();
    format.format(today).toInt
  }

  /**
   * 将yyyyMMddHHmmss格式的字符串转换为yyyyMMdd格式的Int
   */
  def getDateId(x: String): Int = {
    val formatDateid = new SimpleDateFormat("yyyyMMdd")

    try {
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val d = format.parse(x);

      return formatDateid.format(d).toInt
    } catch {
      case e: Exception => println("getDateId parse timestamp wrong")
    }
    0

  }
  
  
  /**
   * 将yyyyMMddHHmmss格式的字符串转换为yyyyMMddHH格式的Int
   */
  def getTimeId(x: String): Int = {
    val formatDateid = new SimpleDateFormat("yyyyMMddHH")

    try {
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val d = format.parse(x);

      return formatDateid.format(d).toInt
    } catch {
      case e: Exception => println("getDateId parse timestamp wrong")
    }
    0

  }

  /**
   * 得到times之前的日期，格式为yyyyMMdd
   */
  def getOldDay(times: Int): String = {

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -times)
    var yesterday = dateFormat.format(cal.getTime())

    yesterday

  }

  /**
   * 将yyyyMMddHHmmss格式的字符串转换为Date类型
   */
  def getDate(x: String): Date = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        return d
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }
    null

  }

  /**
   * 将yyyyMMddHHmmss格式的字符串转换为yyyy-MM-dd HH:mm:ss格式的字符串
   */
  def getTimeStringFormat(x: String): String = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    try {
      if (x == "")
        return null
      else {
        val timeFormat = format2.format(format.parse(x))
        return timeFormat
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }

    null

  }
  /**
   * 将yyyyMMddHHmmss格式的字符串转换为yyyyMMdd格式的String
   */
  def getTimeStringFormatYMD(x: String): String = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val format2 = new SimpleDateFormat("yyyyMMdd")

    try {
      if (x == "")
        return null
      else {
        val timeFormat = format2.format(format.parse(x))
        return timeFormat
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }

    null

  }

  /**
   * 将yyyyMMddHHmmss格式的字符串转换为Timestamp类型
   */
  def getTimestamp(x: String): java.sql.Timestamp = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        val t = new Timestamp(d.getTime());
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }

    null

  }

  /**
   * 将yyyyMMddHHmmss格式的字符串转换为Long类型的时间戳，精确到秒
   */
  def getTimestampLong(x: String): Long = {
    try {
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val d = format.parse(x);
      val t = d.getTime / 1000
      return t
    } catch {
      case e: Exception => println("getTimestampLong parse timestamp wrong")
    }
    0L

  }
  
   /**
   * 将yyyyMMddHHmmss格式的字符串转换为yyyyMMdd235959，然后再转为Long类型的时间戳，精确到秒
   */
  def getTimestampLongFor235959(x: String): Long = {
    val newx = x.substring(0, 8)+"235959"
    try {
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val d = format.parse(x);
      val t = d.getTime / 1000
      return t
    } catch {
      case e: Exception => println("getTimestampLong parse timestamp wrong")
    }
    0L

  }
  
  
  /**
   * 将Long类型的时间戳（精确到毫秒），转换为yyyyMMddHH格式的字符串
   */
  def getyyyyMMddHHFormTimestampLong(x: Long): String = {
    try {
      val date = new Date(x)
      val format = new SimpleDateFormat("yyyyMMddHH")
      return format.format(date)

    } catch {
      case e: Exception => println("getTimestampLong parse timestamp wrong")
    }

    ""
  }

  /**
   * 将Long类型的时间戳（精确到毫秒），转换为yyyyMMddHHmmss格式的字符串
   */
  def getStringFromTimestampLong(x: Long): String = {
    try {
      val date = new Date(x)
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      return format.format(date)

    } catch {
      case e: Exception => println("getTimestampLong parse timestamp wrong")
    }

    ""
  }
  
  /**
   * 求出两个Date类型返回两个时间段的间隔天数
   */
  def getIntervalByDay(startDate: Date , endDate: Date): Int = {
      try {
      val beginTime = startDate.getTime()
      val endTime = endDate.getTime() 
      val betweenDay = ((endTime - beginTime) / (1000 * 60 * 60 *24) + 0.5) 
      val betweenDays = betweenDay.toInt
      return betweenDays

    } catch {
      case e: Exception => println("getTimestampLong parse timestamp wrong")
    }
    0
  }
  
  /**
   * 查询当前日期前(后)x天的日期
   */
  def beforNumberDay(date: Date , day : Int): String ={
     try {
     var c = Calendar.getInstance();
		 c.setTime(date);
		 c.add(Calendar.DAY_OF_YEAR, day);
		 return new SimpleDateFormat("yyyyMMdd").format(c.getTime());

    } catch {
      case e: Exception => println("getTimestampLong parse timestamp wrong")
    }
    ""
  }

}