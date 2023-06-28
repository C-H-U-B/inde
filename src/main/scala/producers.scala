import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.Random
import scala.concurrent.duration._
import play.api.libs.json._

case class Citizen(name: String, harmonyScore: Double, words: List[String])
case class Drone(id: Int, var location: (Int, Int))

object DroneSimulator {
  implicit val citizenWrites: Writes[Citizen] = Json.writes[Citizen]
  implicit val droneWrites: Writes[Drone] = Json.writes[Drone]

  val positiveWords = List("peace", "love", "joy", "harmony", "serenity", "hope", "kindness", "gratitude", "happiness", "contentment")
  val negativeWords = List("anger", "fear", "sadness", "disgust", "envy", "guilt", "shame", "conflict", "stress", "frustration")

  def main(args: Array[String]): Unit = {
    val props = createProperties()
    val producer = new KafkaProducer[String, String](props)

    val harmonyland = Array.fill[Option[Citizen]](100, 100)(None)
    val drones = List.fill(10)(Drone(Random.nextInt(1000), (Random.nextInt(100), Random.nextInt(100))))

    // Place citizens in Harmonyland
    for (_ <- 1 to 1000) {
      val words = List.fill(3)(if (Random.nextBoolean()) positiveWords else negativeWords).map(_.toList(Random.nextInt(10)))
      val harmonyScore = words.map(word => if (positiveWords.contains(word)) 10 else -10).sum
      val citizen = Citizen(s"Citizen${Random.nextInt(1000)}", harmonyScore, words)
      val location = (Random.nextInt(100), Random.nextInt(100))
      harmonyland(location._1)(location._2) = Some(citizen)
    }

    // Simulate drones
    while (true) {
      drones.foreach { drone =>
        moveDrone(drone)
        val droneData = generateDroneData(drone, harmonyland)
        sendRecord(producer, droneData)
      }
      Thread.sleep(1.minute.toMillis) // Wait for 1 minute before the next iteration
    }

    producer.close()
  }

  def createProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def moveDrone(drone: Drone): Unit = {
    // Move the drone to a random adjacent cell
    drone.location = (drone.location._1 + Random.nextInt(3) - 1, drone.location._2 + Random.nextInt(3) - 1)
  }

  def generateDroneData(drone: Drone, harmonyland: Array[Array[Option[Citizen]]]): String = {
    val citizen = harmonyland(drone.location._1)(drone.location._2)
    val data = citizen match {
      case Some(citizen) =>
        Json.obj(
          "drone" -> Json.toJson(drone),
          "citizen" -> Json.toJson(citizen)
        )
      case None =>
        Json.obj(
          "drone" -> Json.toJson(drone)
        )
    }
    Json.stringify(data)
  }

  def sendRecord(producer: KafkaProducer[String, String], droneData: String): Unit = {
    val record = new ProducerRecord[String, String]("droneData", "key", droneData)
    producer.send(record)
  }
}