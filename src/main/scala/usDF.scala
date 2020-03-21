import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\us-500.csv"
    val dff = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    dff.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab where zip =(select max(zip) from tab)")
      res.show()
    spark.stop()
  }
}

