package com.yisa.engine.other
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test8 {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) = {
    var now = new Date().getTime()
    //  var sprakConf = new SparkConf().setAppName("sparksql");
    var sprakConf = new SparkConf().setAppName("test").setMaster("local").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    var sc = new SparkContext(sprakConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val parquetFile = sc.textFile("E://moma//people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()

    //Parquet files can also be registered as tables and then u sed in SQL statements.
    parquetFile.registerTempTable("people")

    sqlContext.udf.register("getlen", (text: String, test2: String) => (1 + 1))
    
    parquetFile.show()

    val teenagers = sqlContext.sql("SELECT name, getlen('bb','aa') FROM people WHERE age >= 13 AND age <= 19")

    teenagers.map(t => "Name: " + t(0)+" len: " + t(1)).collect().foreach(println)
  }
}