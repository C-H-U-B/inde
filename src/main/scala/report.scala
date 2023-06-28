import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import scala.collection.JavaConverters._
import play.api.libs.json._

object DroneReportConsumer {
  implicit val citizenReads: Reads[Citizen] = Json.reads[Citizen]
  implicit val droneReads: Reads[Drone] = Json.reads[Drone]

  def main(args: Array[String]): Unit = {
    val props = createConsumerProperties()
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Collections.singletonList("droneData"))

    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(100)).asScala
      processRecords(records)
    }
  }

  def createConsumerProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def processRecords(records: Iterable[ConsumerRecord[String, String]]): Unit = {
    records.foreach { record =>
      val data = Json.parse(record.value())
      val drone = (data \ "drone").as[Drone]
      val citizenOpt = (data \ "citizen").asOpt[Citizen]
      citizenOpt match {
        case Some(citizen) =>
          println(s"Drone ID: ${drone.id}, Location: ${drone.location}, Citizen: ${citizen.name}, Harmony Score: ${citizen.harmonyScore}, Words: ${citizen.words.mkString(", ")}")
        case None =>
          println(s"Drone ID: ${drone.id}, Location: ${drone.location}, No citizen detected.")
      }
    }
  }
}

