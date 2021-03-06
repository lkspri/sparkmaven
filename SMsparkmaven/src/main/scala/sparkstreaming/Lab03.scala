package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object Lab03 {
  
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
          
          val df1 = df.withColumn("created_dt",current_timestamp())
          
          df1.write.format("jdbc")
          .mode("append")
          .option("url","jdbc:mysql://localhost/custdb")
          .option("user","root")
          .option("password","Root123$")
          .option("dbtable","tblcustomerstream")
          .option("drive","org.mysql.Driver")
          .save()
          
          print("written into mysql")
        }
      })
   
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}