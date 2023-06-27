import java.util.Properties
import org.apache.kafka.clients.producer._

object ProducerApp {
  def main(args: Array[String]): Unit = {
    val props = createProperties()
    val producer = new KafkaProducer[String, String](props)
    val droneData = "droneId,location,citizenNames,harmonyScore,wordsHeard"
    sendRecord(producer, droneData)
    producer.close()
  }

  def createProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def sendRecord(producer: KafkaProducer[String, String], droneData: String): Unit = {
    val record = new ProducerRecord[String, String]("droneData", "key", droneData)
    producer.send(record)
  }
}
