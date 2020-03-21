import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//sparkOozieHive
object sparkOozieHive {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkOozieHive").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val tabs = Array("EMP","DEPT")
    val msurl = "jdbc:sqlserver://venudb.cnwu9xc3aep7.ap-south-1.rds.amazonaws.com:1433;databaseName=venutasks;"
    val msprop = new java.util.Properties()
    msprop.setProperty("user","msusername")
    msprop.setProperty("Password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

    tabs.foreach{ x=>
      val tab = x.toString
      
      val df = spark.read.jdbc(msurl,s"$tab",msprop)
      df.show()

      df.write.mode(SaveMode.Append).format("hive").saveAsTable(s"$tab")
    }


    spark.stop()
  }
}

