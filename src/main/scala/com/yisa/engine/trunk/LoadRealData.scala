package com.yisa.engine.trunk

import org.apache.spark.sql.SparkSession
import com.yisa.engine.uitl.TimeUtil
import com.yisa.engine.branch.SparkEngineV2ForFrequentlyCar

class LoadRealData(sparkSession: SparkSession, hdfsPath: String, dataPath: String, cacheDays: Int, tableName: String, frequentlyCarResultTableName: String, zkHostPort: String) extends Runnable {
  override def run() = {
    var loadData = true;
    while (loadData) {

      val loadData = sparkSession.read.parquet(hdfsPath + dataPath)
      val loadData_filter = loadData
        .filter("platenumber !=  '0' ")
        //      .filter("")
        .filter("platenumber not like  '%未%' ")
        .filter { x => x.getAs[String]("platenumber").length() > 1 }
        .filter { x => x.getAs[String]("platenumber").length() < 20 }
        .filter("dateid >= " + TimeUtil.getLCacheDataDateid(cacheDays))
 
      loadData_filter.createOrReplaceTempView("tmp")
  
      val loadData_filter2 = sparkSession.sql("select  * from tmp  order by capturetime desc")

      sparkSession.catalog.uncacheTable(tableName)
      //    sparkSession.catalog.dropTempView(tableName)
      sparkSession.catalog.clearCache()
      
      sparkSession.catalog.listTables()
      
      loadData_filter.createOrReplaceTempView(tableName)
      sparkSession.catalog.cacheTable(tableName)
      runFirstTask(sparkSession, tableName, frequentlyCarResultTableName, zkHostPort)
      Thread.sleep(10*1000)
    }
  }

  def runFirstTask(sparkSession: SparkSession, tableName: String, frequentlyCarResultTableName: String, zkHostPort: String): Unit = {
    val fcar = new SparkEngineV2ForFrequentlyCar()
    fcar.FrequentlyCar(sparkSession, "01|test1|{\"count\":2}", tableName, frequentlyCarResultTableName, zkHostPort)
  }
}