import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.util.Random
import scala.concurrent.duration._
import play.api.libs.json._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class Citizen(name: String, harmonyScore: Double, words: List[String])
case class Drone(id: Int, location: (Int, Int))

object DroneSimulator {
  implicit val citizenWrites: Writes[Citizen] = Json.writes[Citizen]
  implicit val droneWrites: Writes[Drone] = Json.writes[Drone]

  val positiveWords = List("peace", "love", "joy", "harmony", "serenity", "hope", "kindness", "gratitude", "happiness", "contentment")
  val negativeWords = List("anger", "fear", "sadness", "disgust", "freedom", "democracy", "anarchy", "conflict", "stress", "frustration")

  def main(args: Array[String]): Unit = {
    val props = createProperties()
    val producer = new KafkaProducer[String, String](props)

    val harmonyland = List.fill(100, 100)(Option.empty[Citizen])
    val drones = List.fill(10)(Drone(Random.nextInt(1000), (Random.nextInt(100), Random.nextInt(100))))

    val filledHarmonyland = fillHarmonyland(harmonyland)
    runSimulation(producer, drones, filledHarmonyland)
  }

  def createProperties(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def fillHarmonyland(harmonyland: List[List[Option[Citizen]]]): List[List[Option[Citizen]]] = {
    (1 to 1000).foldLeft(harmonyland) { (h, _) =>
      val words = List.fill(3)(if (Random.nextDouble() < 0.7) positiveWords else negativeWords).map(_.toList(Random.nextInt(10)))
      val harmonyScore = words.map(word => if (positiveWords.contains(word)) 10 else -10).sum
      val citizen = Citizen(s"Citizen${Random.nextInt(1000)}", harmonyScore, words)
      val location = (Random.nextInt(100), Random.nextInt(100))
      h.updated(location._1, h(location._1).updated(location._2, Some(citizen)))
    }
  }

  def runSimulation(producer: KafkaProducer[String, String], drones: List[Drone], harmonyland: List[List[Option[Citizen]]]): Unit = {
    drones.foldLeft(harmonyland) { (h, drone) =>
      val newDrone = moveDrone(drone)
      val newWords = List.fill(3)(if (Random.nextBoolean()) positiveWords else negativeWords).map(_.toList(Random.nextInt(10)))
      val updatedHarmonyland = updateCitizenWords(h, newWords)
      val droneData = generateDroneData(newDrone, updatedHarmonyland)
      sendRecord(producer, droneData)
      updatedHarmonyland
    }
    Thread.sleep(1.minute.toMillis)
    runSimulation(producer, drones, harmonyland)
  }

  def moveDrone(drone: Drone): Drone = {
    drone.copy(location = (drone.location._1 + Random.nextInt(3) - 1, drone.location._2 + Random.nextInt(3) - 1))
  }

  def updateCitizenWords(harmonyland: List[List[Option[Citizen]]], newWords: List[String]): List[List[Option[Citizen]]] = {
    harmonyland.map { row =>
      row.map {
        case Some(citizen) =>
          Some(citizen.copy(words = newWords))
        case None => None
      }
    }
  }

  def generateDroneData(drone: Drone, harmonyland: List[List[Option[Citizen]]]): String = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val citizens = scanZone(drone.location._1, drone.location._2, -5, -5, Nil, harmonyland)

    val data = citizens match {
      case Nil =>
        Json.obj(
          "timestamp" -> timestamp,
          "drone" -> Json.toJson(drone)
        )
      case _ =>
        Json.obj(
          "timestamp" -> timestamp,
          "drone" -> Json.toJson(drone),
          "citizens" -> citizens.map(Json.toJson(_))
        )
    }
    Json.stringify(data)
  }

  def scanZone(x: Int, y: Int, dx: Int, dy: Int, citizens: List[Citizen], harmonyland: List[List[Option[Citizen]]]): List[Citizen] = {
    if (dx > 5) citizens
    else if (dy > 5) scanZone(x, y, dx + 1, -5, citizens, harmonyland)
    else if (x + dx >= 0 && x + dx < 100 && y + dy >= 0 && y + dy < 100) {
      harmonyland(x + dx)(y + dy) match {
        case Some(citizen) => scanZone(x, y, dx, dy + 1, citizen :: citizens, harmonyland)
        case None => scanZone(x, y, dx, dy + 1, citizens, harmonyland)
      }
    } else scanZone(x, y, dx, dy + 1, citizens, harmonyland)
  }

  def sendRecord(producer: KafkaProducer[String, String], droneData: String): Unit = {
    val record = new ProducerRecord[String, String]("droneData", "key", droneData)
    producer.send(record)
  }
}
