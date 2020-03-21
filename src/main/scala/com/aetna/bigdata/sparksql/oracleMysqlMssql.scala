package com.aetna.bigdata.sparksql
//com.aetna.bigdata.sparksql.oracleMysqlMssql
import org.apache.spark.sql._

//
object oracleMysqlMssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracleMysqlMssql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val output = args(0)
    val ourl= "jdbc:oracle:thin:@//paritoshoracle.czhyzqz8vleo.ap-south-1.rds.amazonaws.com:1521/orcl"
    val df1 = spark.read.format("jdbc").option("url",ourl).option("dbtable","EMP").option("driver","oracle.jdbc.OracleDriver").option("user","ousername").option("password","opassword").load()
    df1.show()
    // Another way to get data from oracle database..

    val msurl = "jdbc:sqlserver://venudb.cnwu9xc3aep7.ap-south-1.rds.amazonaws.com:1433;databaseName=venutasks"
    val prop =new java.util.Properties()
    prop.setProperty("user","msusername")
    prop.setProperty("password","mspassword")
    prop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val df2 = spark.read.jdbc(msurl,"dept",prop)
    df2.show()
    df1.createOrReplaceTempView("emp")
    df2.createOrReplaceTempView("dept")
    val query = """ select e.ename,d.dname,d.loc,e.sal,e.job from emp e join dept d on e.deptno=d.deptno where sal >11000"""
    val res = spark.sql(query)
    res.show()

    val murl ="jdbc:mysql://dbmysql.ckyod3xssluv.ap-south-1.rds.amazonaws.com:3306/dbmysql"
    val mprop = new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    res.write.jdbc(murl,output,mprop)
    res.show()



    spark.stop()
  }
}

