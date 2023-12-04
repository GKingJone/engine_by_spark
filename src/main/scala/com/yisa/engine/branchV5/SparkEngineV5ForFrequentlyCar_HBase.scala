package com.yisa.engine.branchV5

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.kafka.clients._
import org.apache.spark.sql.SparkSession
import com.yisa.engine.db.MySQLConnectManager
import com.google.gson.Gson
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.engine.uitl.TimeUtil
import java.text.SimpleDateFormat
import com.yisa.engine.common.InputBean
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
import scala.collection.mutable.ArrayBuffer
import com.yisa.engine.accumulator.FrequentlyCarAccumulatorV2
import com.yisa.engine.db.HBaseConnectManager
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import java.util.regex.Pattern
import java.util.Date
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
import scala.collection.immutable.TreeMap
/**
 * 频繁过车逻辑
 */
class SparkEngineV5ForFrequentlyCar_HBase extends Serializable {

  var map: InputBean = null

  def FrequentlyCarParallelized(sparkSession: SparkSession, line: String, tableName: String, resultTable: String, zkHostport: String, allLocations: ArrayBuffer[String]): Unit = {

    //    val jdbcTable = "pfgc_result_spark"
    val jdbcTable = resultTable

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    map = gson.fromJson[InputBean](params, mapType)

    val startTime = TimeUtil.getTimestampLong(map.startTime)
    val endTime = TimeUtil.getTimestampLong(map.endTime)

    val startTimeDateid = TimeUtil.getDateId(map.startTime)
    val endTimeDateid = TimeUtil.getDateId(map.endTime)

    val carBrand = map.carBrand

    val carModel = map.carModel

    val carLevel = map.carLevel

    val platenumberForMatch = map.plateNumber

    val carColor = map.carColor

    var yearids = Set[String]()
    var carModels = Set[String]()
    var carLevels = Set[String]()
    if (map.carYear != null && map.carYear.size > 0 && map.carYear(0) != "" && map.carYear(0) != "0") {
      map.carYear.foreach { yearid => { yearids = yearids.+(yearid) } }
    }
    if (carModel != null && carModel.length > 0 && map.carModel(0) != "0") {
      carModel.foreach { cm => { carModels = carModels.+(cm) } }
    }

    if (carLevel != null && carLevel.length > 0 && map.carLevel(0) != "0") {
      carLevel.foreach { cl => { carLevels = carLevels.+(cl) } }
    }

    var locationIds: Array[String] = map.locationId
    if (locationIds == null || locationIds.size <= 0 || locationIds(0).equals("0")) {
      locationIds = allLocations.toArray
    }

    //startdateid_enddateid:startTimeLong_endTimeLong
    val indexs = ArrayBuffer[String]()

    locationIds.foreach(location => {
      indexs += (location + "_" + startTime + "_" + endTime)
    })

    println("indexs(0):" + indexs(0))

    /**
     * for (dateid <- startTimeDateid to endTimeDateid) {
     * if (startTimeDateid == endTimeDateid && dateid == startTimeDateid) {
     * indexs += (dateid + "_" + (dateid + 1) + ":" + startTime + "_" + endTime)
     * } else if (startTimeDateid != endTimeDateid && dateid == startTimeDateid) {
     * indexs += (dateid + "_" + (dateid + 1) + ":" + startTime + "_" + (dateid + "235959"))
     * } else if (dateid != endTimeDateid && dateid != startTimeDateid && startTimeDateid != endTimeDateid) {
     * indexs += (dateid + "_" + (dateid + 1) + ":" + (dateid + "000000") + "_" + (dateid + "235959"))
     * } else if (dateid == endTimeDateid && startTimeDateid != endTimeDateid) {
     * indexs += (dateid + "_" + (dateid + 1) + ":" + (dateid + "000000") + "_" + endTime)
     * }
     * }
     *
     */

    val frequentlyCarAccumulatorV2 = new FrequentlyCarAccumulatorV2()
    sparkSession.sparkContext.register(frequentlyCarAccumulatorV2, "FrequentlyCarAccumulatorV2")
    frequentlyCarAccumulatorV2.reset()

    val dataIndexes = sparkSession.sparkContext.parallelize(indexs)
    dataIndexes.foreach { index =>
      //index:locationid_startime_endtime
      {
        val locationid = index.split("_")(0)
        val startTime = index.split("_")(1).toLong
        val endTime = index.split("_")(2).toLong + 1
        val hbaseConn = HBaseConnectManager.getConnet(zkHostport)
        val table_pass_info = hbaseConn.getTable(TableName.valueOf("pass_info_index2"))

        val scan: Scan = new Scan()
        scan.setMaxVersions()

        val startRow = locationid + "_" + startTime
        val endRow = locationid + "_" + endTime

        //        println("startRow:" + startRow)
        //        println("endRow:" + endRow)

        scan.setStartRow(Bytes.toBytes(startRow)).setStopRow(Bytes.toBytes(endRow))

        val rs = table_pass_info.getScanner(scan)

        val iterator = rs.iterator();

        while (iterator.hasNext()) {
          //            println("Scanner hasNext")
          val r = iterator.next()
          //yearid_platenumber_direction_colorid_modelid_brandid_levelid_locationid_lastcaptured_issheltered

          //yearid   0
          //            platenumber   1
          //            direction   2
          //            colorid  3
          //            modelid  4
          //            brandid  5 z
          //            levelid  6
          //            lastcaptured  7
          //            issheltered  8
          //            solrid 9
          if (r != null) {

            val cells: java.util.List[Cell] = r.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("pass"))
            //              val cells: java.util.List[Cell] = r.listCells()

            if (cells != null) {

              for (i <- 0 until cells.size()) {
                val cell = cells.get(i)

                val rowkey = Bytes.toString(CellUtil.cloneRow(cell))

                val locationid = rowkey.split("_")(0)
                val captuetime = rowkey.split("_")(1)

                var value = Bytes.toString(CellUtil.cloneValue(cell))

                //platenumber_solrid_locationid_issheltered_lastcaptured_yearid_dateid
                //yearid   0
                //            platenumber   1
                //            direction   2
                //            colorid  3
                //            modelid  4
                //            brandid  5
                //            levelid  6
                //            lastcaptured  7
                //            issheltered  8
                //            solrid 9
                val one_line = value.split("_")

                val colorid_line = one_line(3)
                val modelid_line = one_line(4)
                val brandid_line = one_line(5)
                val levelid_line = one_line(6)
                val yearid_line = one_line(0)
                val platenumber_line = one_line(1)

                //                  println(platenumber_line)

                var addOrNot = true;

                if (addOrNot && platenumber_line.contains("无") || platenumber_line.contains("?")) {
                  addOrNot = false
                }

                if (addOrNot && platenumberForMatch != null && !platenumberForMatch.equals("")) {
                  var MatchingRule = platenumberForMatch.replace("?", ".{1}?").replace("*", ".*")

                  // ?替换.{1}?
                  // *替换.*

                  var pattern = Pattern.compile(MatchingRule);
                  var matcher = pattern.matcher(platenumber_line);
                  addOrNot = matcher.matches();
                }

                if (addOrNot && carColor != null && carColor != "" && carColor != "0") {
                  if (!colorid_line.equals(carColor))
                    addOrNot = false
                }

                if (addOrNot && carBrand != null && carBrand != "" && carBrand != "0") {
                  if (!brandid_line.equals(carBrand))
                    addOrNot = false
                }

                if (addOrNot && yearids.size > 0) {
                  if (!yearids.contains(yearid_line)) {
                    addOrNot = false
                  }
                }

                if (addOrNot && carModels.size > 0) {
                  if (!carModels.contains(modelid_line)) {
                    addOrNot = false
                  }
                }

                if (addOrNot && carLevels.size > 0) {
                  if (!carLevels.contains(levelid_line)) {
                    addOrNot = false
                  }
                }
                if (addOrNot) {
                 

                  //yearid   0
                  //            platenumber   1
                  //            direction   2
                  //            colorid  3
                  //            modelid  4
                  //            brandid  5
                  //            levelid  6
                  //            lastcaptured  7
                  //            issheltered  8
                  //            solrid 9
                  frequentlyCarAccumulatorV2.add(locationid + "_"
                    + one_line(1) + "_" + one_line(9))
                }
              }
            }
          }
        }
        table_pass_info.close()

      }
    }

    var resultMap: HashMap[String, HashSet[String]] = frequentlyCarAccumulatorV2.value

    var treeMap: TreeMap[Int, ArrayBuffer[String]] = new TreeMap[Int, ArrayBuffer[String]]()

    resultMap.keys.foreach(location_platenumber => {

      val solridsBylocation_platenumber = resultMap(location_platenumber)
      val locationid = location_platenumber.split("_")(0)
      val oneSolrid = solridsBylocation_platenumber.head

      val TotalReverse = Int.MaxValue - solridsBylocation_platenumber.size

      if (treeMap.contains(TotalReverse)) {

        val solrids = treeMap(TotalReverse)
        solrids.+=(oneSolrid + "_" + locationid)

      } else {
        treeMap = treeMap.+(TotalReverse -> ArrayBuffer[String](oneSolrid + "_" + locationid))

      }
    })

    var conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    var sql = "insert into " + jdbcTable + " (s_id,count,j_id,l_id) values(?,?,?,?)";
    var pstmt = conn.prepareStatement(sql);
    var count = 0

    import scala.util.control.Breaks._
    breakable {

      treeMap.foreach(m => {
        val total = Int.MaxValue - m._1

        val oneSolrid_locationid_array = m._2

        if (total >= map.count) {

          oneSolrid_locationid_array.foreach(oneSolrid_locationid => {

            val solrid = oneSolrid_locationid.split("_")(0)
            val locationid = oneSolrid_locationid.split("_")(1)

            pstmt.setString(1, solrid);
            pstmt.setInt(2, total);
            pstmt.setString(3, jobId);
            pstmt.setString(4, locationid);
            pstmt.addBatch();
            count += 1
            if (count > 1000)
              break;

          })

        }

      })

      //
      //      resultMap.foreach(m => {
      //        //          key locationid_platenumber -> set[solrid]
      //        val total = m._2.size
      //        if (total >= map.count) {
      //          val locationid_platenumber = m._1
      //          val locationid = locationid_platenumber.split("_")(0)
      //          val platenumber = locationid_platenumber.split("_")(1)
      //
      //          pstmt.setString(1, m._2.head);
      //          pstmt.setInt(2, total);
      //          pstmt.setString(3, jobId);
      //          pstmt.setString(4, locationid);
      //          pstmt.addBatch();
      //          count += 1
      //
      //        }
      //
      //      })
    }

    if (count == 0) {
      count = -1
    }
    println("count" + count)

    var sql2 = "insert into pfgc_count (j_id,count) values(?,?)";
    var pstmt2 = conn.prepareStatement(sql2);
    pstmt2.setString(1, jobId);
    pstmt2.setInt(2, count);
    pstmt2.executeUpdate()
    pstmt2.close()

    pstmt.executeBatch()
    conn.commit()
    pstmt.close()

    conn.close()

  }

}

