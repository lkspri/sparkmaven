package structurestreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date

object Lab01 {
  
  def main(arg:Array[String])=
  {
    val spark = SparkSession.builder().appName("lob01").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.readStream.format("rate").option("rowPerSecond",1).load()
    
    val df1 = df.withColumn("current_dt", current_date())
    
    df1.writeStream.format("console").start().awaitTermination()
    
  }
  
}