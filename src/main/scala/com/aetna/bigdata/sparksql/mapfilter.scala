package com.aetna.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mapfilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mapfilter").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\work\\datasets\\asldata.txt"
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()
    // sell * from tab where age>40
    //val pro = usrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._2>40)
  val pro = usrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).filter(x=>x._2=="MUM")
    pro.collect.foreach(println)
    spark.stop()
  }
}

