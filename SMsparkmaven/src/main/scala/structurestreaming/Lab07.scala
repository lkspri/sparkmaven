package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Lab07 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab07-structstream").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
     
    val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transtopic")
    .option("group.id", "grptest")
    .load().select(col("value").cast("string"))
    
    val df1 =  df.select(split(col("value"), ",").alias("stud"))
    
    val df2 = df1.select(col("stud").getItem(0).alias("Studid"),col("stud").getItem(1).alias("StudName"),col("stud").getItem(2).alias("Mark1"),
        col("stud").getItem(3).alias("Mark2"),col("stud").getItem(4).alias("Mark3"))
    
    val df3 = df2.withColumn("current_dt", current_date())
    
    val df4 = df3.withColumn("TotalMarks", col("Mark1") + col("Mark2") + col("Mark3") )
    
    val df5 = df4.withColumn("value", concat_ws(",",col("Studid"),col("StudName"),col("Mark1"),col("Mark2"),col("Mark3"),col("Mark1"),col("TotalMarks")))
    
    
    //kafka sink
    //It pushes data from the value column in the dataframe
    
    df5.writeStream.format("kafka")
     .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "tk7")
     .option("checkpointLocation", "file:/tmp/sparkchkpoint")
    .outputMode("append") //For file sink, it supports only append
    .start()
    .awaitTermination()
    
  }
  
}