package sparkstreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Lab09 {
  
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Lab08-Streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
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
      
    
    
    
    ssc.start()
    ssc.awaitTermination()
  
  
  }
  
}