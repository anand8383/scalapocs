package com.aetna.bigdata.sparksql
//com.aetna.bigdata.sparksql.wordcount
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object wordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
   /* val data = "C:\\work\\datasets\\wcdata.txt"
    val wcc = sc.textFile(data)
    val pro = wcc.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)

    pro.take(10).foreach(println)*/
   // val data = "C:\\work\\datasets\\us-500.csv"
    val data = args(0)
    val op = args(1)
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()
    val res = usrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(6).replaceAll("\"",""),1)).
      reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    res.take(10).foreach(println)
    res.coalesce(1).saveAsTextFile(op)

    //val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    spark.stop()
  }
}

