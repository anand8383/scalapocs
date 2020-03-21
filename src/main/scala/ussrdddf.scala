import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.length

object ussrdddf {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ussrdddf").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\us-500.csv"
    //val data = args(0)
    //val op = args(1)
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val res = usrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"","")
      ,x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"","")
      ,x(10).replaceAll("\"",""),x(11).replaceAll("\"",""))).toDF("first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web")
    res.createOrReplaceTempView("ustab")
    //val pro = spark.sql("select email from ustab where email like ('%gmail.com') or email like ('%hotmail.com')")
    //val pro = spark.sql("select email from ustab order by SUBSTRING(email,(CHARINDEX('@',email)+1),1)")
   // val query =""" select RIGHT(email, length(email) - instr(email,'@')) domain, count(email) emailcount from ustab where length(email)>0 group by RIGHT(email, length(email) - instr(email,'@')) order by emailcount desc """
     //   val pro = spark.sql(query)
    val pro = spark.sql("select * from ustab ")

    pro.show(100,false)
    spark.stop()
  }
}//20-5

/*
SELECT RIGHT(Email, LEN(Email) - CHARINDEX('@', email)) Domain ,
COUNT(Email) EmailCount
FROM   dbo.email
WHERE  LEN(Email) > 0
GROUP BY RIGHT(Email, LEN(Email) - CHARINDEX('@', email))
ORDER BY EmailCount DESC
 */