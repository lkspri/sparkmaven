package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession

object Lab02 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder.appName("lab02").master("local").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    val dstream1 = ssc.textFileStream("file:/home/hduser/custlandingpath")
    
    val dstream2 = dstream1.map(x => x.split(","))
    
    val dstream3 = dstream2.filter(x => x.length == 5)
    
    val dstream4 = dstream3.map(x => (x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
    dstream4.foreachRDD(rdd =>
      {
        if(!rdd.isEmpty())
        {
          import spark.implicits._
          val df = rdd.toDF("custid","fname","lname","age","prof")
          df.show()
        }
      })
   
    ssc.start()
    ssc.awaitTermination()
    }
    }
  