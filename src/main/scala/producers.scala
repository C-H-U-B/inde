import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.Random

object DroneSimulator {
  def main(args: Array[String]): Unit = {
    val props = createProperties()
    val producer = new KafkaProducer[String, String](props)

    // Simulate data for 100 drones
    simulateDrones(producer, 100)

    producer.close()
  }

  def createProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def generateDroneData(): String = {
    val droneId = Random.nextInt(1000) // Generate a random drone ID
    val location = s"${Random.nextInt(100)},${Random.nextInt(100)}" // Generate a random location
    val citizenNames = List.fill(5)(s"Citizen${Random.nextInt(1000)}").mkString(",") // Generate random citizen names
    val harmonyScore = Random.nextDouble() * 100 // Generate a random harmony score
    val wordsHeard = List.fill(5)(s"Word${Random.nextInt(1000)}").mkString(",") // Generate random words heard
    s"$droneId,$location,$citizenNames,$harmonyScore,$wordsHeard"
  }

  def sendRecord(producer: KafkaProducer[String, String], droneData: String): Unit = {
    val record = new ProducerRecord[String, String]("droneData", "key", droneData)
    producer.send(record)
  }

  def simulateDrones(producer: KafkaProducer[String, String], numDrones: Int): Unit = {
    if (numDrones > 0) {
      val droneData = generateDroneData()
      sendRecord(producer, droneData)
      simulateDrones(producer, numDrones - 1)
    }
  }
}
