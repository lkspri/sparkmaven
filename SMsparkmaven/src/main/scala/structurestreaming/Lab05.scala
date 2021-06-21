package structurestreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.types._

object Lab05 {
  
  def main(arg:Array[String])=
  {
    val spark = SparkSession.builder().appName("lob05").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    /*val schema = StructType(List(
        StructField("studid",IntegerType,true),
        StructField("studname",StringType,true),
        StructField("m1",IntegerType,true),
        StructField("m2",IntegerType,true),
        StructField("m3",IntegerType,true)
        ))
        * 
        */
    
    val df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","transtopic")
    .option("group.id","geptest")
    .load().select(col("value").cast("string"))
    
    val df1 = df.select(split(col("value"),",").alias("stud"))
    
    val df2 = df1.select(col("stud").getItem(0).alias("Studid"),col("stud").getItem(1).alias("Studname"),
        col("stud").getItem(2).alias("Mark1"),
        col("stud").getItem(3).alias("Mark2"),col("stud").getItem(3).alias("Mark3"))
    
    val df3 = df2.withColumn("current_dt", current_date())
    
    val df4 = df3.withColumn("TotalMarks",col("Mark1") + col("Mark2") + col("Mark3"))
    
    df4.writeStream.format("console").start().awaitTermination()
    
  }
  
}