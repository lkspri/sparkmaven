package structurestreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Lab04
{
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Lab04").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val bookschema = StructType(List(
        StructField("title",StringType,true),
        StructField("author",StringType,true),
        StructField("year_written",IntegerType,true),
        StructField("edition",StringType,true),
        StructField("price",DoubleType,true)
        )) 
        
    val df = spark.readStream
                  .format("json")
                  .schema(bookschema)
                  .option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
                  .option("path","file:/home/hduser/sparkstream")
                  .load()
    df.createOrReplaceTempView("tblbooks")
    
    val df1 = spark.sql("select * from tblbooks where year_written > 1900")
    
    df1.writeStream.format("console").start().awaitTermination()
    
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
  
*/
