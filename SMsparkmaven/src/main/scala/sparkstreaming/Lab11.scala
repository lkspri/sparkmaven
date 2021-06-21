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

object Lab11 {
  
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder.appName("lab11").master("local").getOrCreate()
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
      
      def convertarraytojson(ar:Array[String])=
      {
        s"""{"txnid":"${ar(0)}","txndate":"${ar(1)}","txncustid":"${ar(2)}","txnamt":"${ar(3)}","txncategory":"${ar(4)}",
          "txnproduct":"${ar(5)}","txncity":"${ar(6)}","txnstate":"${ar(7)}","txnpaytype":"${ar(8)}"}"""
      }

        val dstream5 = dstream4.map(convertarraytojson)
      
      
      ssc.start()
      ssc.awaitTermination()
  }
  
}