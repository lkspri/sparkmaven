package sparkkafka

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._

object KConsumer1 {
   def main(args:Array[String])=
  {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    
    val topics = util.Arrays.asList("tk3","tk1")
    
    consumer.subscribe(topics)
    
    while (true) 
    {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        println(data)        
    }
   
  }
}