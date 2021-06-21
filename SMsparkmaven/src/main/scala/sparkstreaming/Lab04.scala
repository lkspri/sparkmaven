package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object Lab04 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder.appName("lab04").master("local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    val dstream = ssc.socketTextStream("localhost",9999)
    dstream.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
  
}