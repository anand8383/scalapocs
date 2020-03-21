import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object dataFunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("dataFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val df= spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "dob", "sal", "comm", "deptno")

    val ndf1 = df.withColumn("dob",to_date($"dob","dd-MMM-y")).withColumn("today",current_date())
    val ndf = ndf1.withColumn("date_diff",datediff($"today",$"dob")).
      orderBy($"date_diff".desc).withColumn("addmonths",add_months($"today",3))
      .withColumn("date_add",date_add($"dob",20))
        .withColumn("dateformat",to_date(date_format(current_date(),"dd-MMM-yy"),"dd-MMM-yy"))
    ndf.printSchema()
    ndf.show(false)
    spark.stop()
  }
}

