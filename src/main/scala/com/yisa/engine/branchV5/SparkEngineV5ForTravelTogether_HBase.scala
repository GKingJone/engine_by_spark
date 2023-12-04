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
import com.yisa.engine.common.Locationid_detail
import com.yisa.engine.common.Locationid_detail
import com.yisa.engine.common.Locationid_detail
import java.util.ArrayList
import com.yisa.engine.db.HBaseConnectManager
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import com.yisa.engine.accumulator.Together2AccumulatorV2
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import java.sql.Timestamp
import java.util.Date
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hbase.client.Get
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.SolrQuery
import com.yisa.engine.uitl.SolrUtils
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.SolrServerException
import java.io.IOException

/**
 * 同行车辆
 *
 *
 * 注意：
 * 1、如果传入请求内有车型等信息，可能会出现，不传车型时，某个车牌出现，传了车型之后，车牌反而出不来。是因为这个车牌的每一条过车记录不是都识别出了车
 * 车型等信息。当限制车型之后，此车牌的过车量变少了，此时，调低“次数”值 ，这个车牌会再次出现
 *
 * 2、程序内取的Yarid是最大值，也就是说当某个车牌被识别为多个yearid时，只会返回最大的那个
 *
 */
class SparkEngineV5ForTravelTogether_HBase extends Serializable {

  def TravelTogether(sparkSession: SparkSession, line: String, tableName: String, zkHostport: String, url: String): Unit = {

    val jdbcTable = "togetherCar_result"

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    println(params)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val travelTogether: InputBean = gson.fromJson[InputBean](params, mapType)

    val countT = travelTogether.count

    val diff = travelTogether.differ

    val accumulator = new Together2AccumulatorV2()
    sparkSession.sparkContext.register(accumulator, "Together2AccumulatorV2")
    accumulator.reset()

    val indexs = ArrayBuffer[String]()
    var query1 = "A009:[" + travelTogether.startTime + " TO " + travelTogether.endTime + "] AND A007:" + travelTogether.plateNumber

    if (travelTogether.locationId != null && travelTogether.locationId.length != 0 && travelTogether.locationId(0) != "" && travelTogether.locationId(0) != "0") {
      var l = travelTogether.locationId.reduce((a, b) => a + " OR " + b)
      query1 = query1 + " AND A002:(" + l + " )"
    }
    val result_docs = query(query1, url)

    //    println("query1:" + query1)

    //    println("result_docs.getNumFound.toInt:" + result_docs.getNumFound.toInt)

    for (i <- 0 until result_docs.getNumFound.toInt) {
      if (i < result_docs.getNumFound.toInt && i < 1000) {
        val doc = result_docs.get(i)
        indexs += (doc.get("id").toString())
      }
    }

    /**
     * val hbaseConn = HBaseConnectManager.getConnet(zkHostport)
     * val table_vehicleinfos = hbaseConn.getTable(TableName.valueOf("vehicleinfos"))
     * for (i <- 0 until result_docs.getNumFound.toInt - 1) {
     * val doc = result_docs.get(i)
     * println("doc.get" + doc.get("id"))
     * val hbaseRowkey = doc.get("id").toString()
     *
     * println("hbaseRowkey:" + hbaseRowkey)
     *
     * val get = new Get(Bytes.toBytes(hbaseRowkey));
     * get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A009"));
     * get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A002"));
     *
     * val result = table_vehicleinfos.get(get);
     *
     * if (result != null) {
     *
     * val location_id_Cell = result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("A002"))
     * val capture_time_Cell = result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("A009"))
     *
     * if (location_id_Cell != null && capture_time_Cell != null) {
     *
     * val scan: Scan = new Scan()
     * scan.setMaxVersions()
     *
     * val capturetime = Bytes.toLong(CellUtil.cloneValue(capture_time_Cell)).toString()
     * val locationid = Bytes.toString(CellUtil.cloneValue(location_id_Cell))
     * println("capturetime:" + capturetime)
     * println("locationid:" + locationid)
     *
     * val format = new SimpleDateFormat("yyyyMMddHHmmss")
     *
     * val d = format.parse(capturetime);
     * val capturetime_Long = d.getTime / 1000
     * println("capturetime_Long:" + capturetime_Long)
     *
     * val startRow = capturetime_Long - diff
     * val endRow = capturetime_Long + diff
     *
     * for (o <- startRow until endRow) {
     * indexs += (locationid + "_" + o)
     * }
     *
     * }
     * }
     * }
     *
     *
     *
     */

    //
    //    val query2 = query1
    //    val options = Map(
    //      "collection" -> "vehicle",
    //      "zkhost" -> "27.10.20.6:2181/whsolr6",
    //      "query" -> query2)
    //    println(query2)
    //
    //    val df = sparkSession.read
    //      .format("solr")
    //      .options(options) 
    //      .load

    var yearids = Set[String]()
    var carModels = Set[String]()
    var carLevels = Set[String]()
    if (travelTogether.carYear != null && travelTogether.carYear.size > 0 && travelTogether.carYear(0) != "" && travelTogether.carYear(0) != "0") {
      travelTogether.carYear.foreach { yearid => { yearids = yearids.+(yearid) } }
    }

    val carBrand = travelTogether.carBrand

    val carModel = travelTogether.carModel

    val carLevel = travelTogether.carLevel

    val platenumber_self = travelTogether.plateNumber

    val carColor = travelTogether.carColor

    if (carModel != null && carModel.length > 0 && travelTogether.carModel(0) != "0") {
      carModel.foreach { cm => { carModels = carModels.+(cm) } }
    }

    if (carLevel != null && carLevel.length > 0 && travelTogether.carLevel(0) != "0") {
      carLevel.foreach { cl => { carLevels = carLevels.+(cl) } }
    }

    val dataIndexes = sparkSession.sparkContext.parallelize(indexs)
    dataIndexes.foreach { hbaseRowkey =>
      //	long  	A009;	//capture_time
      val hbaseConn = HBaseConnectManager.getConnet(zkHostport)
      val table_pass_info = hbaseConn.getTable(TableName.valueOf("pass_info_index2"))
      val table_vehicleinfos = hbaseConn.getTable(TableName.valueOf("vehicleinfos"))

      //      val hbaseRowkey = row.getString(row.length - 1)

      //      println("hbaseRowkey:" + hbaseRowkey)

      val get = new Get(Bytes.toBytes(hbaseRowkey));
      get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A009"));
      get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A002"));

      val result = table_vehicleinfos.get(get);
      if (result != null) {

        val location_id_Cell = result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("A002"))
        val capture_time_Cell = result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("A009"))

        if (location_id_Cell != null && capture_time_Cell != null) {

          val scan: Scan = new Scan()
          scan.setMaxVersions()

          val capturetime = Bytes.toLong(CellUtil.cloneValue(capture_time_Cell)).toString()
          val locationid = Bytes.toString(CellUtil.cloneValue(location_id_Cell))
          //          println("capturetime:" + capturetime)
          //          println("locationid:" + locationid)

          val format = new SimpleDateFormat("yyyyMMddHHmmss")

          val d = format.parse(capturetime);
          val capturetime_Long = d.getTime / 1000
          //          println("capturetime_Long:" + capturetime_Long)

          val startTime = capturetime_Long - diff
          val endTime = capturetime_Long + diff + 1

          val startRow = locationid + "_" + startTime
          val endRow = locationid + "_" + endTime

          //          println("startRow:" + startRow)
          //          println("endRow:" + endRow)

          scan.setStartRow(Bytes.toBytes(startRow)).setStopRow(Bytes.toBytes(endRow))

          //          println("startRow" + startRow)
          //          println("endRow" + endRow)

          val rs = table_pass_info.getScanner(scan)
          while (rs.iterator().hasNext()) {
            //            println("Scanner hasNext")
            val r = rs.next()
            //yearid_platenumber_direction_colorid_modelid_brandid_levelid_locationid_lastcaptured_issheltered

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
            if (r != null) {

              val cells: java.util.List[Cell] = r.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("pass"))
              //              val cells: java.util.List[Cell] = r.listCells()

              if (cells != null) {

                for (i <- 0 until cells.size()) {
                  val cell = cells.get(i)

                  val rowkey = Bytes.toString(CellUtil.cloneRow(cell))

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

                  if (platenumber_self.equals(platenumber_line) || platenumber_line.contains("无") || platenumber_line.contains("?")) {
                    addOrNot = false
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
                    val date = new Date((rowkey.split("_")(1) + "000").toLong)
                    val format = new SimpleDateFormat("yyyyMMdd")
                    val dateid = format.format(date).toInt

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
                    accumulator.add(one_line(1) + "_"
                      + one_line(9) + "_"
                      + rowkey.split("_")(0) + "_"
                      + one_line(8) + "_"
                      + one_line(7) + "_"
                      + one_line(0) + "_"
                      + dateid)
                  }

                  //                  println("addOrNot:" + addOrNot)

                }
              }
            }
          }
        }
      }
      table_vehicleinfos.close()
      table_pass_info.close()
    }

    val treeMap: TreeMap[String, Map[String, ArrayBuffer[String]]] = accumulator.value

    println("treeMap:" + treeMap.size)

    val conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    val sql = new StringBuffer()
    sql.append("insert into ").append(jdbcTable)
    sql.append(" (")
    sql.append("j_id,s_id,plate_number,days,locationid_detail,locationid_count,total,is_sheltered,last_captured,yearid")
    sql.append(")").append("values(?,?,?,?,?,?,?,?,?,?)");

    //    println("insert result sql:" + sql.toString())
    val pstmt_result = conn.prepareStatement(sql.toString());
    val sql2 = "insert into togetherCar_count (j_id,count) values(?,?)";
    val pstmt_count = conn.prepareStatement(sql2);

    try {
      treeMap.foreach(plate => {

        val plate_number = plate._1
        val plate_map = plate._2

        //        println("plate_number:" + plate_number)

        val gson = new Gson()
        val total = plate_map.get("solrid").get.toSet.size.toLong

        if (countT < total) {

          var locationids = plate_map.get("locationid").get
          var locationid_m = Map[String, Int]()

          locationids.foreach { locationid =>
            {

              if (locationid_m.contains(locationid)) {
                locationid_m = locationid_m.+(locationid -> (locationid_m.get(locationid).get + 1))
              } else {
                locationid_m = locationid_m.+(locationid -> 1)
              }

            }
          }

          val locationid_detail_List = new ArrayList[Locationid_detail]();
          val locationid_count = locationid_m.size

          locationid_m.foreach(e => {
            val (k, v) = e
            val locationid_detail = Locationid_detail.apply(k, v);
            locationid_detail_List.add(locationid_detail)

          })

          val solrids = plate_map.get("solrid").get.toSet.reduce((a, b) => a + "," + b)

          //foreach { solrid: String => solrids = solrids + "," + solrid }

          pstmt_result.setString(1, jobId);
          pstmt_result.setString(2, solrids);
          pstmt_result.setString(3, plate_number);
          pstmt_result.setInt(4, plate_map.get("dateid").get.toSet.size);
          pstmt_result.setString(5, gson.toJson(locationid_detail_List));
          pstmt_result.setInt(6, locationid_count);
          pstmt_result.setInt(7, (total + "").toInt);

          val issheltereds = plate_map.get("issheltered").get.toSet.max
          pstmt_result.setInt(8, issheltereds.toInt);

          val lastcaptured = plate_map.get("issheltered").get.toSet.max

          if (lastcaptured != null) {
            pstmt_result.setInt(9, lastcaptured.toInt);
          } else {
            pstmt_result.setInt(9, 0);
          }

          val yearid = plate_map.get("yearid").get

          if (yearid != null) {
            pstmt_result.setInt(10, yearid.head.toInt);
          } else {
            pstmt_result.setInt(10, -1);
          }

          pstmt_result.addBatch();

        }

      })

      pstmt_result.executeBatch()
      conn.commit()
      pstmt_result.close()

      pstmt_count.setString(1, jobId)
      if (treeMap != null && treeMap.size >= 1) {

        pstmt_count.setInt(2, 1)

      } else {
        pstmt_count.setInt(2, -1)
      }
      pstmt_count.executeUpdate()
      conn.commit()
      pstmt_count.close()

    } catch {
      case e: Exception => {
        println("task wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        println(e.printStackTrace())
      }
    } finally {
      conn.close()
    }

  }

  def registTmpAllTable(travelTogethers: InputBean, sparkSession: SparkSession, tableName: String, locationidsTable: String): String = {

    val startTimeDateid = TimeUtil.getDateId(travelTogethers.startTime)
    val endTimeDateid = TimeUtil.getDateId(travelTogethers.endTime)

    val sql = new StringBuffer();
    sql.append("SELECT *  ").append(" FROM ").append(tableName);
    sql.append(" WHERE ")

    sql.append("  capturetime   >= ").append(TimeUtil.getTimestampLong((travelTogethers.startTime)));
    sql.append(" AND capturetime   <= ").append(TimeUtil.getTimestampLong((travelTogethers.endTime)));

    sql.append(" AND locationid IN (").append(" select locationid from ").append(locationidsTable).append(")")

    sql.append(" AND dateid >= ").append(startTimeDateid)

    sql.append(" AND dateid <= ").append(endTimeDateid)

    val tmpTable = "pass_info_tmp_all";

    val resultData_1 = sparkSession.sql(sql.toString());

    println(sql.toString())

    resultData_1.createOrReplaceTempView(tmpTable);

    return tmpTable;

  }

  def registTmpTableSQL(travelTogethers: InputBean, sparkSession: SparkSession, tableName: String): String = {

    val startTimeDateid = TimeUtil.getDateId(travelTogethers.startTime)
    val endTimeDateid = TimeUtil.getDateId(travelTogethers.endTime)

    val sql = new StringBuffer();
    sql.append("SELECT locationid,capturetime ").append(" FROM ").append(tableName);
    sql.append(" WHERE platenumber = '").append(travelTogethers.plateNumber).append("'");

    sql.append(" AND capturetime   >= ").append(TimeUtil.getTimestampLong((travelTogethers.startTime)));
    sql.append(" AND capturetime   <= ").append(TimeUtil.getTimestampLong((travelTogethers.endTime)));

    if (travelTogethers.locationId != null && travelTogethers.locationId.length != 0 && travelTogethers.locationId(0) != "" && travelTogethers.locationId(0) != "0") {
      var l = travelTogethers.locationId.map { "'" + _ + "'" }.reduce((a, b) => a + "," + b)
      sql.append(" AND locationid IN (").append(l).append(")")
    }

    sql.append(" AND dateid >= ").append(startTimeDateid)

    sql.append(" AND dateid <= ").append(endTimeDateid)

    return sql.toString();

  }

  def getSQL(travelTogethers: InputBean, js: String, tableName: String, tmpTable: String): String = {

    val startTime = TimeUtil.getTimestampLong(travelTogethers.startTime)
    val endTime = TimeUtil.getTimestampLong(travelTogethers.endTime)

    val startTimeDateid = TimeUtil.getDateId(travelTogethers.startTime)
    val endTimeDateid = TimeUtil.getDateId(travelTogethers.endTime)

    val sb = new StringBuffer()
    sb.append("  SELECT solrids,platenumber,days, locationids,total,issheltered,lastcaptured,yearid ")
    sb.append("  FROM (")
    sb.append(" SELECT concat_ws(',',collect_set(o.solrid )) AS solrids ,o.platenumber,")
    sb.append(" size(collect_set(o.dateid )) AS days,collect_list(o.locationid) AS locationids, ")
    sb.append(" count(o.solrid) AS total,max(o.issheltered) AS issheltered,max(o.lastcaptured) AS lastcaptured,")
    sb.append(" max(o.yearid) AS yearid  ")

    sb.append(" FROM  ( ")

    sb.append(" SELECT distinct p.solrid,p.platenumber,p.dateid,p.locationid,p.solrid,p.issheltered,p.lastcaptured,p.yearid")

    sb.append(" FROM ").append(tableName).append(" p , ").append(tmpTable).append(" t ")

    //    sb.append(" JOIN  ").append(" ON p.locationid = t.locationid ")
    sb.append(" WHERE  ").append("  p.locationid = t.locationid ")

    sb.append(" AND   platenumber not like  '%无%' AND  platenumber not like  '%未%'  AND  platenumber not like  '%牌%'  AND  platenumber not like  '%?%'  AND  platenumber not like  '%00000%'  ")

    sb.append(" AND platenumber != '" + travelTogethers.plateNumber + "' ")

    sb.append(" AND p.capturetime - t.capturetime  >= ").append((0 - travelTogethers.differ))
    sb.append(" AND p.capturetime - t.capturetime  <= ").append(travelTogethers.differ)

    if (travelTogethers.carYear != null && travelTogethers.carYear.size > 0 && travelTogethers.carYear(0) != "" && travelTogethers.carYear(0) != "0") {
      var m = travelTogethers.carYear.reduce((a, b) => a + "," + b)
      sb.append(" AND yearid IN (").append(m).append(")");
    }

    if (startTime != 0L) {
      sb.append(" AND p.capturetime >= ").append(startTime)
    }

    if (endTime != 0L) {
      sb.append(" AND p.capturetime <= ").append(endTime)
    }

    if (startTime != 0L) {
      sb.append(" AND dateid >= ").append(startTimeDateid)
    }

    if (endTime != 0L) {
      sb.append(" AND dateid <= ").append(endTimeDateid)
    }

    val carBrand = travelTogethers.carBrand

    val carModel = travelTogethers.carModel

    val carLevel = travelTogethers.carLevel

    val carColor = travelTogethers.carColor

    if (carModel != null && carModel.length > 0 && travelTogethers.carModel(0) != "0") {
      var m = carModel.reduce((a, b) => a + "," + b)
      sb.append(" AND modelid IN (").append(m).append(")");
    }

    if (carBrand != null && carBrand != "" && carBrand != "0") {
      sb.append(" AND brandid = ").append(carBrand)
    }
    if (carLevel != null && carLevel.length > 0 && travelTogethers.carLevel(0) != "0") {
      var m = carLevel.reduce((a, b) => a + "," + b)
      sb.append(" AND levelid IN (").append(m).append(")");
    }

    if (carColor != null && carColor != "" && carColor != "0") {
      sb.append(" AND colorid = ").append(carColor)
    }

    sb.append(" ) o ")

    sb.append("  WHERE   platenumber not like  '%无%' AND  platenumber not like  '%未%'  AND  platenumber not like  '%牌%'  AND  platenumber not like  '%?%'  AND  platenumber not like  '%00000%'  ")

    sb.append(" GROUP BY  platenumber ")
    //    sb.append("  limit 10000 ")

    sb.append(" ) t ")
    sb.append(" WHERE t.total >= ").append(travelTogethers.count)

    sb.toString()
  }

  def query(mQueryStr: String, url: String): SolrDocumentList = {

    try {
      val httpSolrClient: HttpSolrClient = SolrUtils.connect(url);
      val query: SolrQuery = new SolrQuery();
      //设定查询字段  
      query.setQuery(mQueryStr);
      //指定返回结果字段  
      query.set("fl", "id");
      //覆盖schema.xml的defaultOperator（有空格时用"AND"还是用"OR"操作逻辑），一般默认指定。必须大写  
      query.set("q.op", "AND");
      //设定返回记录数，默认为10条  
      query.setRows(1000);
      val response: QueryResponse = httpSolrClient.query(query);
      val list: SolrDocumentList = response.getResults();
      return list;
    } catch {
      case e: SolrServerException => {
        println("task wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        println(e.printStackTrace())

      }
      case e: IOException => {
        println("task wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        println(e.printStackTrace())
      }
      case e: Exception => {
        println("task wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        println(e.printStackTrace())

      }

    }
    return null;
  }

  def TravelTogetherGetFromFile(sparkSession: SparkSession, line: String, tableName: String, zkHostport: String): Unit = {

    val jdbcTable = "togetherCar_result"

    var line_arr = line.split("\\|")

    val jobId = line_arr(1)
    val params = line_arr(2)

    println(params)

    val gson = new Gson
    val mapType = new TypeToken[InputBean] {}.getType
    val travelTogether: InputBean = gson.fromJson[InputBean](params, mapType)

    val countT = travelTogether.count

    val diff = travelTogether.differ

    val accumulator = new Together2AccumulatorV2()
    sparkSession.sparkContext.register(accumulator, "Together2AccumulatorV2")
    accumulator.reset()

    //    val end = new Date().getTime
    val tmpTableSQL: String = registTmpTableSQL(travelTogether, sparkSession, tableName)

    val resultData_1 = sparkSession.sql(tmpTableSQL)

    //SELECT locationid,capturetime
    resultData_1.foreachPartition { rows =>

      val hbaseConn = HBaseConnectManager.getConnet(zkHostport)

      rows.foreach { row =>
        {

          val table = hbaseConn.getTable(TableName.valueOf("pass_info_index2"));
          val scan: Scan = new Scan()
          scan.setMaxVersions()

          val capturetime = row.getLong(1)
          val locationid = row.getString(0)
          val startRow = locationid + "_" + (capturetime - diff)
          val endRow = locationid + "_" + (capturetime + diff)
          scan.setStartRow(Bytes.toBytes(startRow)).setStopRow(Bytes.toBytes(endRow))

          val rs = table.getScanner(scan)
          while (rs.iterator().hasNext()) {
            val r = rs.next()
            //yearid_platenumber_direction_colorid_modelid_brandid_levelid_locationid_lastcaptured_issheltered

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
            if (r != null) {

              val cells: java.util.List[Cell] = r.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("pass"))

              if (cells != null) {

                for (i <- 0 until cells.size()) {
                  val cell = cells.get(i)

                  val rowkey = Bytes.toString(CellUtil.cloneRow(cell))

                  var value = Bytes.toString(CellUtil.cloneValue(cell))
                  //platenumber_solrid_locationid_issheltered_lastcaptured_yearid_dateid

                  val one_line = value.split("_")

                  val date = new Date((rowkey.split("_")(1) + "000").toLong)
                  val format = new SimpleDateFormat("yyyyMMdd")
                  val dateid = format.format(date).toInt

                  accumulator.add(one_line(1) + "_"
                    + one_line(9) + "_"
                    + rowkey.split("_")(0) + "_"
                    + one_line(8) + "_"
                    + one_line(7) + "_"
                    + one_line(0) + "_"
                    + dateid)

                }
              }
            }
          }
          table.close()

        }

      }
    }

    val treeMap: TreeMap[String, Map[String, ArrayBuffer[String]]] = accumulator.value

    println("treeMap:" + treeMap.size)

    val conn = MySQLConnectManager.getConnet(zkHostport)
    conn.setAutoCommit(false)
    val sql = new StringBuffer()
    sql.append("insert into ").append(jdbcTable)
    sql.append(" (")
    sql.append("j_id,s_id,plate_number,days,locationid_detail,locationid_count,total,is_sheltered,last_captured,yearid")
    sql.append(")").append("values(?,?,?,?,?,?,?,?,?,?)");

    //    println("insert result sql:" + sql.toString())
    val pstmt_result = conn.prepareStatement(sql.toString());
    val sql2 = "insert into togetherCar_count (j_id,count) values(?,?)";
    val pstmt_count = conn.prepareStatement(sql2);

    try {
      treeMap.foreach(plate => {

        val plate_number = plate._1
        val plate_map = plate._2

        //        println("plate_number:" + plate_number)

        val gson = new Gson()
        val total = plate_map.get("solrid").get.toSet.size.toLong

        var locationids = plate_map.get("locationid").get
        var locationid_m = Map[String, Int]()

        locationids.foreach { locationid =>
          {

            if (locationid_m.contains(locationid)) {
              locationid_m = locationid_m.+(locationid -> (locationid_m.get(locationid).get + 1))
            } else {
              locationid_m = locationid_m.+(locationid -> 1)
            }

          }
        }

        val locationid_detail_List = new ArrayList[Locationid_detail]();
        val locationid_count = locationid_m.size

        locationid_m.foreach(e => {
          val (k, v) = e
          val locationid_detail = Locationid_detail.apply(k, v);
          locationid_detail_List.add(locationid_detail)

        })

        val solrids = plate_map.get("solrid").get.toSet.reduce((a, b) => a + "," + b)

        //foreach { solrid: String => solrids = solrids + "," + solrid }

        pstmt_result.setString(1, jobId);
        pstmt_result.setString(2, solrids);
        pstmt_result.setString(3, plate_number);
        pstmt_result.setInt(4, plate_map.get("dateid").get.toSet.size);
        pstmt_result.setString(5, gson.toJson(locationid_detail_List));
        pstmt_result.setInt(6, locationid_count);
        pstmt_result.setInt(7, (total + "").toInt);
        pstmt_result.setInt(8, plate_map.get("issheltered").get.head.toInt);

        val lastcaptured = plate_map.get("issheltered").get

        if (lastcaptured != null) {
          pstmt_result.setInt(9, lastcaptured.head.toInt);
        } else {
          pstmt_result.setInt(9, 0);
        }

        val yearid = plate_map.get("yearid").get

        if (yearid != null) {
          pstmt_result.setInt(10, yearid.head.toInt);
        } else {
          pstmt_result.setInt(10, -1);
        }

        pstmt_result.addBatch();

      })

      pstmt_result.executeBatch()
      conn.commit()
      pstmt_result.close()

      pstmt_count.setString(1, jobId)
      if (treeMap != null && treeMap.size >= 1) {

        pstmt_count.setInt(2, 1)

      } else {
        pstmt_count.setInt(2, -1)
      }
      pstmt_count.executeUpdate()
      conn.commit()
      pstmt_count.close()

    } catch {
      case e: Exception => {
        println("task wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        println(e.getMessage)
        println(e.printStackTrace())
      }
    } finally {
      conn.close()
    }

  }

}













//object TogetherCounter {
//
//  @volatile private var instance: LongAccumulator = null
//
//  def getInstance(sc: SparkContext): LongAccumulator = {
//    if (instance == null) {
//      synchronized {
//        if (instance == null) {
//          instance = sc.longAccumulator("TogetherCounter")
//        }
//      }
//    }
//    instance
//  }
//}


