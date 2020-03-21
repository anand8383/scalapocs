import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object unstructureRDD {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("unstructureRDD").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\asldata.txt"
    val aslrdd = sc.textFile(data)
    val skip = aslrdd.first()
    //By Default RDD is unstructure, thus convert into proper sturcture using map and filter. next convert to dataframe using toDF()
    val clean = aslrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
    clean.printSchema()
    clean.createOrReplaceTempView("tab")
    val ssk = spark.sql("select city, count(*) cnt  from tab group by city order by cnt desc")
    ssk.show()

    spark.stop()
  }
}

