package sparkusecase

import org.apache.spark.{SparkContext,SparkConf}

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

import org.apache.spark.sql.SaveMode

object usecase03 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().master("local[*]").appName("uc-03").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.read.format("csv")
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","profession")
    
    df.write.format("jdbc").option("url","jdbc:mysql://localhost/custdb")
    .mode("append")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","ret_custs")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .save()
    
    println("Customer data loaded into mysql")
    
     val df1 = spark.read.format("csv")
    .load("file:/home/hduser/hive/data/txns")
    .toDF("txnid","txndate","custid","txnamt","product","category","city","state","ptype")
    
    df1.write.format("jdbc").option("url","jdbc:mysql://localhost/custdb")
    .mode("overwrite")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","ret_txns")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .save()
    
    println("Transaction data loaded into mysql")
    
     val df2 = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","ret_custs")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .load()
    
    val df3 = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","ret_txns")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .load()
    
    df2.createOrReplaceTempView("custs")
    
    df3.createOrReplaceTempView("txns")
    
    val df4 = spark.sql("""select state,profession,txndate,sum(txnamt) as totalsales,count(txnid) as noofsales 
        from txns t inner join custs c on t.custid = c.custid group by state,profession,txndate order by 
        txndate desc,totalsales asc""")
    
    df4.write.format("csv").mode(SaveMode.Append)
    .option("delimiter","|")
    .save("hdfs://localhost:54310/tmp/transreport")
    
     println("Written into the hdfs filesystem")
    
   
  }
  
}