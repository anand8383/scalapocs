import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace


object sparkFunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data ="C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true")load(data)
    //df.show()
    df.createOrReplaceTempView("tab")
    //val query = """select Upper(city) as city ,count(*)cnt from tab group by city"""
    //val query = """select city, collect_list(product) item from tab group by city """
    //val query = """select upper(city),collect_set(product) item from tab group by city"""
    //val query ="""select upper(first_name),last_name, concat_ws(' ', first_name,last_name) as fullname, city, state,email from tab"""
    //val res = df.select upper(
    //val query = """ select * from tab"""
    //def toUpper
    val reg = "^[0-9a-zA-Z]"
   // val query =""" select first_name, split(address,'') newaddress, regexp_replace(phone1,'-','') phone, regexp_replace(address,',','') address, lpad(zip,20,'0') newzip from tab"""
    def offers(st:String,em:String)= (st,em) match {
      case(s,e) if(s=="OH")=> "5% off"
      case(s,e) if(s=="_" && e.contains("gmail.com"))=> "50% offer"
        case(s,e) if(s=="NJ")=>"40% offer"
      case _=> "no offer"
    }
    val todayoff = udf(offers _)
    spark.udf.register("off",todayoff)
    val test = df.withColumn("test12",todayoff($"state",$"email"))
    val query =""" select *,off(state,email) todayoffer from tab"""
    val res = spark.sql(query)
    res. show(100,false)
/* def offer(st:String): String = st match {
      case "OH" => "10% off"
      case "NJ" => "20% off"
      case "NY" => "30% off"
      case _ => " no offer"*/

    spark.stop()
  }
}

