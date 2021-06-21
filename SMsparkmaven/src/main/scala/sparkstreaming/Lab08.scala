package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object Lab08 {
  
   def main(args:Array[String])=
  {
    val spark = SparkSession.builder.appName("lab05").master("local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    ssc.checkpoint("file:/tmp/sparkcheckpoint")
    
    val dstream1 = ssc.socketTextStream("localhost",8888)
    //dstream1.print()
    
    val dstream2 = dstream1.flatMap(x => x.split(" "))
    
    val dstream3 = dstream2.map(x => (x,1))
        
    val dstream4 = dstream3.window(Seconds(15),Seconds(10))
    dstream4.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
   
}