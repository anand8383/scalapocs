import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkfunc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkfunc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\us-500.csv"
    val df= spark.read.format("csv").option("header","true").option("inferSchema","true").load(data).withColumnRenamed("Company_name","companyname")
    df.printSchema
    df.createOrReplaceTempView("tab")
    val query = """select * from tab"""
    val dff=spark.sql(query)
    def offer(st:String): String = st match {
      case "OH" => "10% off"
      case "NJ" => "20% off"
      case "NY" => "30% off"
      case _ => " no offer"
    }
    val off = udf(offer _)

    val col = df.columns.map(x=>x.toUpperCase())
    val ndf = df.toDF(col:_*).withColumn("todayoffer",off($"state"))
    //val ndf = dff.withColumn("age",lit("25")).withColumn("phone1",regexp_replace($"phone1","-","")).withColumn("phone2",regexp_replace($"phone2","-",""))
      //  .where($"state"==="NJ")
    ndf.show(false)

    spark.stop()
  }
}

