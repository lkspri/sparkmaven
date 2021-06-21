package sparkkafka

import java.util.Properties
import org.apache.kafka.clients.producer._

object KProducer {
  
  def main(args:Array[String])=
  {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    
    val file = scala.io.Source.fromFile("/home/hduser/hive/data/custs").getLines()
    
    for(f <- file)
    {
    
      //create message
      val record = new ProducerRecord[String, String]("tk4", null, f)
      
      //send message
      producer.send(record)
    }
    println("Written into kafka topic tk4")
    producer.close()
  }
  
}