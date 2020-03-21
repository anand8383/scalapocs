import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparktask {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparktask").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\100000 Records.csv"
    val source = spark.read.format("csv").option("header","true").option("inferSchema","true").option("dateFormat","dd/mm/yyyy")load(data)
    val reg = "[^a-zA-Z0-9]"
    val cols = source.columns.map(x=>x.replaceAll(reg,"")).map(x=>x.toLowerCase())
    val df = source.toDF(cols:_*)
    //val rrdd = spark.read.format("csv").option("inferSchema","true").option("header","true").option("dateFormat","dd/mm/yyyy").load(data).withColumnRenamed("E Mail","email").withColumnRenamed("Last  Hike","LastHike").withColumnRenamed("Date of Joining","dateofjoining")
    df.createOrReplaceTempView("tab")
    df.printSchema()
    val query ="""select * from tab"""
    val sss = spark.sql(query)
    sss.show(50,false)

    //val ss = spark.sql("select * from tab e where last hike >= ( select max(salary) from tab where salary>e.salary) order by salary desc")
    //val ss = spark.sql("select [Salary],[Last % Hike] from tab")
    //val query =""" select RIGHT(email, length(email) - instr(email,'@')) domain, count(email) emailcount from ustab where length(email)>0 group by RIGHT(email, length(email) - instr(email,'@')) order by emailcount desc """
    //   val pro = spark.sql(query)
    //val query1 ="""select * from tab"""
    //val s
    //val query ="""select RIGHT(email, length(email)- instr(email, '@')) domain, count(email) emailcount from tab where length(email)>0 group by RIGHT(email, length(email) - instr(email,'@')) order by emailcount desc"""
   /* val query =
    """ select Max('First Name') KEEP(DENSE_RANK FIRST ORDER BY dateofjoining ) as youngest,
      | Max('First Name') KEEP(DENSE_RANK FIRST ORDER BY dateofjoining ) AS Oldest from tab
      |""".stripMargin
    val ss= spark.sql(query)
    ss.show(100,false)*/
    spark.stop()
  }
}

