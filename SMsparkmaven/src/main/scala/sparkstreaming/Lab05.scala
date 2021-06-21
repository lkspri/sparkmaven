package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object Lab05 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder.appName("lab05").master("local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    val dstream1 = ssc.socketTextStream("localhost",8888)
    dstream1.print()
    
    val dstream2 = dstream1.map(x => x.split(","))
    
    val dstream3 = dstream2.filter(x => x.length == 5)
    
    val dstream4 = dstream3.map(x => (x(0).toInt,x(1),x(2).toInt,x(3).toFloat,x(4).toInt))
    
    val dstream5 = dstream4.transform(rdd => rdd.sortBy(x => x._3,true))
    
    dstream5.print()
   
    ssc.start()
    ssc.awaitTermination()
  }
  
}