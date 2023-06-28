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

    consumeRecords(consumer)
  }

  def createConsumerProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "reportGroup")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def consumeRecords(consumer: KafkaConsumer[String, String]): Unit = {
    val records = consumer.poll(java.time.Duration.ofMillis(100)).asScala.toList
    if (records.nonEmpty) {
      processRecords(records)
      consumer.commitSync()
    }
    consumeRecords(consumer)
  }

  def processRecords(records: List[ConsumerRecord[String, String]]): Unit = records match {
    case record :: remainingRecords =>
      val data = Json.parse(record.value())
      val timestamp = (data \ "timestamp").as[String]
      val drone = (data \ "drone").as[Drone]
      val citizens = (data \ "citizens").asOpt[Seq[Citizen]].getOrElse(Seq.empty[Citizen])

      if (citizens.nonEmpty) {
        citizens.foreach { citizen =>
          println(s"Timestamp: $timestamp, Drone ID: ${drone.id}, Location: ${drone.location}, Citizen: ${citizen.name}, Harmony Score: ${citizen.harmonyScore}, Words: ${citizen.words.mkString(", ")}")
        }
      } else {
        println(s"Timestamp: $timestamp, Drone ID: ${drone.id}, Location: ${drone.location}, No citizen detected.")
      }

      processRecords(remainingRecords)

    case Nil => // No more records to process
  }
}
