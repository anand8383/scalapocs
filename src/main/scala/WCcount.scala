import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WCcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("WCcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\us-500.csv"
    val wrdd = sc.textFile(data)
    val skip = wrdd.first()
    //val reg = ",(?=([^\"]*\"[^\"*\")*[^\"]*$)"
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
         //   ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val ress = wrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(7).replaceAll("\"",""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    ress.take(100).foreach(println)
//replaceAll("\"",""),1))
    spark.stop()
  }
}

