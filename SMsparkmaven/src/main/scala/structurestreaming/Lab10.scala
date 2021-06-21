package structurestreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger


object Lab10
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab10").master("local").getOrCreate()
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
    
    val df5 = df4.groupBy("Studid","StudName").agg(sum("Mark1"))
    
    //default output mode is append
    df4.writeStream
    .format("console")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start().awaitTermination()
    
    //complete mode is only used for aggregated data
    /*df5.writeStream
    .format("console")
    .outputMode("complete")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start().awaitTermination()  
    *   
    */
    
    /*df5.writeStream
    .format("console")
    .outputMode("update")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start().awaitTermination()  
    *   
    */
    
  }
}

/*
  
 Read from Input sources -> Transformation -> Write into Sink 
 
Input Sources:
=============

Rate (for Testing): It will automatically generate data including 2 columns timestamp and value . This is generally used for testing purposes. 
Socket: This data source will listen to the specified socket and ingest any data into Spark Streaming.
File: This will listen to a particular directory as streaming data. It supports file formats like CSV, JSON, ORC, and Parquet.
Kafka: This will read data from Apache Kafka® and is compatible with Kafka broker versions 0.10.0 or higher 


Sink Types:
========== 

Console sink: Displays the content of the DataFrame to console
File sink: Stores the contents of a DataFrame in a file within a directory. Supported file formats are csv, json, orc, and parquet.
Kafka sink: Publishes data to a Kafka topic
Foreachbatch: allows you to specify a function that is executed on the output data of every micro-batch of the streaming query.

Triggers:
=========

Default: Executes a micro-batch as soon as the previous finishes
Fixed interval micro-batches: Specifies the interval when the micro-batches will execute. For example, 1 minute , 30 seconds or 1 hour etc
One-time micro-batch: Executes only one micro-batch to process all available data and then stops.

Output Modes:
=============

Append mode (default) - This is the default mode, where only the new rows added to the Result Table since the last trigger will be outputted to the sink. 
Complete mode - The whole Result Table will be outputted to the sink after every trigger. This is supported for aggregation queries.
Update mode - It is similar to the complete with one exception; update output mode outputMode("update") just outputs the updated aggregated results every time to data sink when new data arrives. 
but not the entire aggregated results like complete mode. 
If the streaming data is not aggregated then, it will act as append mode.
  
*/
