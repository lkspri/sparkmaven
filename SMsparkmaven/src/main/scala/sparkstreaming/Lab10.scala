package sparkstreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Lab10 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder.appName("lab010").master("local").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
     val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "test1"
          )
    
      val topics = Array("transtopic")
      
      val dstream1 = KafkaUtils.createDirectStream[String, String](ssc,LocationStrategies.PreferBrokers,Subscribe[String, String](topics, kafkaParams))
      
      val dstream2 = dstream1.map(x => x.value())
      
      dstream2.print()
          
      val dstream3 = dstream2.map(x => x.split(","))
    
      val dstream4 = dstream3.filter(x => x.length == 9)
    
      val dstream5 = dstream4.map(x => (x(0).toInt,x(1),x(2).toInt,x(3).toFloat,x(4),x(5),x(6),x(7),x(8)))
    
      dstream4.foreachRDD(rdd =>
      {
        if(!rdd.isEmpty())
        {
          import spark.implicits._
          val df = rdd.toDF("txnid","txndate","txncustid","txnamt","txncategory","txnproduct","txncity","txnstate","txnpaytype")
          
          df.write.format("jdbc")
          .mode("append")
          .option("url","jdbc:mysql://localhost/custdb")
          .option("user","root")
          .option("password","Root123$")
          .option("dbtable","tbltranskafka")
          .option("drive","org.mysql.Driver")
          .save()
          
        }
      })
      
    
    
    
    ssc.start()
    ssc.awaitTermination()
  
  
  }
  
}