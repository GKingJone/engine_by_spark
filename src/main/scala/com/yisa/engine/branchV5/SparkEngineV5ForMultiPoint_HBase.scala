package com.yisa.engine.branchV5

import org.apache.spark.sql.SparkSession
import com.yisa.engine.common.InputBean
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.text.SimpleDateFormat
import com.yisa.engine.uitl.TimeUtil
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.yisa.engine.db.HBaseConnectManager
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import com.yisa.engine.accumulator.MultiPointAccumulatorV2
import util.control.Breaks._
import com.yisa.engine.db.MySQLConnectManager
import java.util.regex.Pattern

class SparkEngineV5ForMultiPoint_HBase extends Serializable {
  
  def MultiPointParallelized(sparkSession: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    //注册累加器
    val accumulatorMultiPoint = new MultiPointAccumulatorV2()
    sparkSession.sparkContext.register(accumulatorMultiPoint, "MultiPointAccumulator")
    accumulatorMultiPoint.reset()

    var line_arr = line.split("\\|")

    val hbaseTableName = "pass_info_index2"

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[Array[InputBean]] {}.getType
    val map: Array[InputBean] = gson.fromJson[Array[InputBean]](params, mapType)

    var inputBeanRepair: InputBean = null

    //生成rowkey
    val rowkeys = getRowkeys(map)

    // Hbase分布式计算
    val disRowkeys = sparkSession.sparkContext.parallelize(rowkeys)
    disRowkeys.foreach { rowkey =>

      var locaId = rowkey.split("_")(0)
      var timeId = rowkey.split("_")(1)
      var taskId = rowkey.split("_")(2)

      var rowkeyOne = locaId + "_" + timeId
      val hbaseConn = HBaseConnectManager.getConnet(zkHostport)
      val table = hbaseConn.getTable(TableName.valueOf(hbaseTableName));
      val get = new Get(Bytes.toBytes(rowkeyOne));
      get.setMaxVersions();

      val result = table.get(get);

      if (result != null) {

        val results = new HashMap[String, String]()

        // yearid_platenumber_direction_colorid_modelid_brandid_levelid_lastcaptured_issheltered_solrid_taskId_timeId
        val cells: java.util.List[Cell] = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("pass"))

        if (cells != null) {

          for (i <- 0 until cells.size()) {
            val cell = cells.get(i)
            var value = Bytes.toString(cell.getValue).toString()
            var version = cell.getTimestamp.toString()
            results += (version -> value)
          }

        }

        var otherStr = "_" + taskId + "_" + timeId
        val resultsAll = new HashMap[String, String]()
        results.foreach(e => {
          var (k, v) = e
          accumulatorMultiPoint.add(v + otherStr)
        })

      }

      table.close()
    }

    // 解析数据
    // 获取合并完的数据
    val accuMap = accumulatorMultiPoint.value

    // 满足条件的数据
    var anayMap = new HashMap[String, String]()
    accuMap.foreach(e => {
      var (k, v) = e
      var isOKResult = isOK(map, k, v)
      if (isOKResult) {
        anayMap += (k -> v)
      } else {
        println("isOK-error")
      }

    })

    println("满足条件的数据：" + anayMap.size)
    // 获取可能存在的排除条件和合并taskid
    if (anayMap.size > 0) {

      // 获得排除条件
      var inputIs: InputBean = null
      map.foreach { x =>
        if (x.isRepair == 1) {
          inputIs = x
        }
      }

      // 合并taskid
      // key platenumber
      // value solrid_taskid
      var margeMap = new HashMap[String, String]()
      anayMap.foreach(e => {
        var (k, v) = e

        var platenumber = k.split("_")(0)
        var taskId = k.split("_")(1)

        var solrid = v.split("_")(0)
        val capturetime = v.split("_")(7).toLong

        if (margeMap.contains(platenumber)) {
          var margeMapV = margeMap.get(platenumber).get
          var margeT = margeMapV.split("_")(0)
          var margeS = margeMapV.split("_")(1)
          var margeC = margeMapV.split("_")(2).toLong

          if (capturetime > margeC) {
            // 时间大
            var margeTs = margeT + "," + taskId
            var margeRe = margeTs + "_" + solrid + "_" + capturetime.toString()
            margeMap += (platenumber -> margeRe)
          } else {
            var margeTs = margeT + "," + taskId
            var margeRe = margeTs + "_" + margeS + "_" + margeC
            margeMap += (platenumber -> margeRe)
          }
        } else {
          var margeRe = taskId + "_" + solrid + "_" + capturetime.toString()
          margeMap += (platenumber -> margeRe)
        }
      })

      println("合并taskid：" + margeMap.size)
      // 排除条件的场合
      var endResultMap = HashMap[String, String]()
      if (inputIs != null) {
        var inputIsTask = inputIs.differ

        var inputIsCount = inputIs.count

        // 排除不符合的车牌
        var isTmpMap = HashMap[String, String]()
        margeMap.foreach(e => {
          var (k, v) = e
          var isTaskIds = v.split("_")(0)
          var isSolr = v.split("_")(1)
          if (!isTaskIds.contains(inputIsTask)) {
            var isValues = isTaskIds + "_" + isSolr
            isTmpMap += (k -> isValues)
          }
        })

        // 条件数》=2
        isTmpMap.foreach(e => {
          var (k, v) = e
          var isTaskIds = v.split("_")(0)
          var isSolr = v.split("_")(1)
          if (isTaskIds.contains(",")) {
            var endSize = isTaskIds.split(",").size

            //            if (inputIsCount > 2) {
            //              inputIsCount = inputIsCount - 1
            //            }

            if (endSize >= inputIsCount) {
              var isValues = isTaskIds + "," + inputIsTask.toString() + "_" + isSolr
              endResultMap += (k -> isValues)
            }
          }
        })
      } else {
        // 符合条件场合
        margeMap.foreach(e => {
          var inBean = map(0)
          var inBeanCount = inBean.count

          var (k, v) = e
          var isTaskIds = v.split("_")(0)

          //          if (inBeanCount > 2) {
          //            inBeanCount = inBeanCount - 1
          //          }

          if (isTaskIds.contains(",")) {

            var inBeanSize = isTaskIds.split(",").size
            if (inBeanSize >= inBeanCount) {
              endResultMap += (k -> v)
            }
          }
        })

      }

      // 插入数据库
      println("合并taskid：" + margeMap.size)
      insertDB(endResultMap, zkHostport, jobId)
    }

  }

  // 插入数据库
  def insertDB(map: HashMap[String, String], zkHostport: String, jobId: String): Unit = {

    var conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    var sql = "insert into zdypz_result (s_id,j_id,t_id) values(?,?,?)";
    var pstmt = conn.prepareStatement(sql);

    var number = 0;
    map.foreach { t =>
      {

        var (k, v) = t
        number = number + 1

        pstmt.setString(1, v.split("_")(1));
        pstmt.setString(2, jobId);
        pstmt.setString(3, v.split("_")(0));
        pstmt.addBatch();
      }

    }

    var sql2 = "insert into zdypz_progress (jobid,total) values(?,?)";
    var pstmt2 = conn.prepareStatement(sql2);

    if (number == 0) {
      number = -1
    }
    pstmt2.setString(1, jobId);
    pstmt2.setInt(2, number);
    pstmt2.executeUpdate()
    pstmt2.close()

    pstmt.executeBatch()
    conn.commit()
    pstmt.close()

    conn.close()

  }

  // 判断是否符合条件
  def isOK(map: Array[InputBean], key: String, value: String): Boolean = {

    var isRight = true

    println("mutil" + key)
    println("mutil" +value)
    //val key = platenumber + "_" + taskid
    //val value = solrid + "_" + levelid + "_" + brandid+ "_" + modelid+ "_" + yearid+ "_" + direction+ "_" + colorid+ "_" + capturetime

    var platenumber = key.split("_")(0)
    var taskId = key.split("_")(1).toInt

    var solrid = value.split("_")(0)
    var levelid = value.split("_")(1)
    var brandid = value.split("_")(2)
    var modelid = value.split("_")(3)
    var yearid = value.split("_")(4)
    var direction = value.split("_")(5)
    var colorid = value.split("_")(6)
    var capturetime = value.split("_")(7)

    map.foreach { inputBean =>

      if (inputBean.differ == taskId) {

        // 判断车辆类别
        if (inputBean.carLevel != null && inputBean.carLevel.length > 0) {

          inputBean.carLevel.foreach { x =>
            if (x.equals(levelid)) {
              isRight = false
            }
          }

          // 符合条件
          if (!isRight) {
            isRight = true
          } else {
            // 不符合条件
            isRight = false
          }

        }

        // 判断车辆品牌
        if (isRight && inputBean.carBrand != null && !"".equals(inputBean.carBrand)) {

          if (inputBean.carBrand.equals(brandid)) {
            isRight = false
          }

          // 符合条件
          if (!isRight) {
            isRight = true
          } else {
            // 不符合条件
            isRight = false
          }

        }

        // 判断车辆型号
        if (isRight && inputBean.carModel != null && inputBean.carModel.size > 0) {

          inputBean.carModel.foreach { x =>
            if (x.equals(modelid)) {
              isRight = false
            }
          }

          // 符合条件
          if (!isRight) {
            isRight = true
          } else {
            // 不符合条件
            isRight = false
          }
        }

        // 判断车辆年款
        if (isRight && inputBean.carYear != null && inputBean.carYear.size > 0) {

          inputBean.carYear.foreach { x =>
            if (x.equals(yearid)) {
              isRight = false
            }
          }

          // 符合条件
          if (!isRight) {
            isRight = true
          } else {
            // 不符合条件
            isRight = false
          }

        }

        // 判断行驶方向
        if (isRight && inputBean.direction != null && !"".equals(inputBean.direction)) {

          if (inputBean.direction.equals(direction)) {
            isRight = false
          }

          // 符合条件
          if (!isRight) {
            isRight = true
          } else {
            // 不符合条件
            isRight = false
          }
        }

        // 判断车身颜色
        if (isRight && inputBean.carColor != null && !"".equals(inputBean.carColor)) {

          if (inputBean.carColor.equals(colorid)) {
            isRight = false
          }

          // 符合条件
          if (!isRight) {
            isRight = true
          } else {
            // 不符合条件
            isRight = false
          }

        }

        // 判断车牌号码
        if (isRight && inputBean.plateNumber != null && !"".equals(inputBean.plateNumber)) {

          if (platenumber != null && !"".equals(platenumber)) {
            var old = inputBean.plateNumber.replace("?", ".{1}?").replace("*", ".*")
            var com = platenumber

            // ?替换.{1}?
            // *替换.*

            var pattern = Pattern.compile(old);
            var matcher = pattern.matcher(com);
            isRight = matcher.matches();

          } else {
            isRight = false
          }

        }

      }
    }

    return isRight
  }

  // 生产rowkey
  def getRowkeys(map: Array[InputBean]): ArrayBuffer[String] = {

    val rowkeys = ArrayBuffer[String]()
    //    val map5 = new HashMap[String,Int]()
    map.foreach { inputBean =>

      var locationId: Array[String] = inputBean.locationId

      val format2 = new SimpleDateFormat("yyyyMMddHHmmss")

      var taskId = inputBean.differ

      var startTime2 = format2.parse(inputBean.startTime).getTime / 1000
      var endTime2 = format2.parse(inputBean.endTime).getTime / 1000

      var keyStart = startTime2;
      for (keyStart <- startTime2 to endTime2) {
        locationId.foreach { locaId =>
          var rowkey = locaId + "_" + keyStart.toString() + "_" + taskId;
          rowkeys += rowkey
          //          map5 += (rowkey -> taskId)
        }
      }

      var direction: String = inputBean.direction
    }

    return rowkeys
  }
}