import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConverters._
import play.api.libs.json._

object RiotDetector {
  implicit val citizenReads: Reads[Citizen] = Json.reads[Citizen]
  implicit val droneReads: Reads[Drone] = Json.reads[Drone]

  def main(args: Array[String]): Unit = {
    val consumerProps = createConsumerProperties()
    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(java.util.Collections.singletonList("droneData"))

    val producerProps = createProducerProperties()
    val producer = new KafkaProducer[String, String](producerProps)

    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(100)).asScala
      processRecords(records, producer)
    }
  }

  def createConsumerProperties(): Properties = {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "alertGroup")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props
}


  def createProducerProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def processRecords(records: Iterable[ConsumerRecord[String, String]], producer: KafkaProducer[String, String]): Unit = {
  records.foreach { record =>
    val data = Json.parse(record.value())
    val drone = (data \ "drone").as[Drone]
    val citizens = (data \ "citizens").asOpt[Seq[Citizen]].getOrElse(Seq.empty[Citizen])

    citizens.foreach { citizen =>
      if (citizen.harmonyScore < 0) {
        val alert = s"Riot detected at location ${drone.location} by drone ${drone.id}. Citizen ${citizen.name} has a harmony score of ${citizen.harmonyScore}."
        println(alert)
        val alertRecord = new ProducerRecord[String, String]("riotAlerts", "alert", alert)
        producer.send(alertRecord)
      }
    }
  }
}

}
