package sparkusecase

import org.apache.spark.sql.SparkSession

object usecase01 
{
  def main(args:Array[String])=
  {
    
    val spark = SparkSession.builder().appName("usecase01-SQL")
    .config("spark.sql.warehouse.dir","file:/tmp/warehouse")
    .master("local").enableHiveSupport().getOrCreate()
     
    spark.sparkContext.setLogLevel("ERROR")
    
     val df = spark.read.format("jdbc")
    .option("url","jdbc:mysql://localhost/custdb")
    .option("user","root")
    .option("password","Root123$")
    .option("dbtable","tblorderdata")
    .option("driver","com.mysql.cj.jdbc.Driver").load()
  
    df.write.mode("append").saveAsTable("tblorderhive1")
    
  }
}