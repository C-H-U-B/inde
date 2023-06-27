import java.util._
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object ConsumerApp {
  def main(args: Array[String]): Unit = {
    val props = createProperties()
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Arrays.asList("droneData"))
    consumeRecords(consumer)
  }

  def createProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def consumeRecords(consumer: KafkaConsumer[String, String]): Unit = {
    val records = consumer.poll(100)
    records.asScala.foreach(record => println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}"))
    consumeRecords(consumer)
  }
}
