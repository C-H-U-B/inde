import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import scala.collection.JavaConverters._
import play.api.libs.json._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.regions.Region

object DroneReportConsumer {
  implicit val citizenReads: Reads[Citizen] = Json.reads[Citizen]
  implicit val droneReads: Reads[Drone] = Json.reads[Drone]

  val s3 = S3Client.builder().region(Region.EU_WEST_1).build() // Change the region as needed
  val bucketName = "inde-bucket" // Change this to your bucket name

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
      
      val timestampOption = (data \ "timestamp").asOpt[String]
      println(s"Processing record with timestamp $timestampOption")

      val drone = (data \ "drone").as[Drone]
      val citizens = (data \ "citizens").asOpt[Seq[Citizen]].getOrElse(Seq.empty[Citizen])

      val csvLines = if (citizens.nonEmpty) {
        citizens.map { citizen =>
          s"$timestampOption,${drone.id},${drone.location._1},${drone.location._2},${citizen.name},${citizen.harmonyScore},${citizen.words.mkString(", ")}"
        }
      } else {
        List(s"$timestampOption,${drone.id},${drone.location._1},${drone.location._2},No citizen detected.")
      }

      val csvContent = csvLines.mkString("\n")
      val csvPath = Files.createTempFile("drone_report_", ".csv")
      Files.write(csvPath, csvContent.getBytes(StandardCharsets.UTF_8))

      val putRequest = PutObjectRequest.builder().bucket(bucketName).key(csvPath.getFileName.toString).build()
      s3.putObject(putRequest, csvPath)

      processRecords(remainingRecords)

    case Nil => // No more records to process
  }
}