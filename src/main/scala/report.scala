import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.common.serialization.StringDeserializer
import play.api.libs.json._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.regions.Region

object DroneReportConsumerSpark {
  val spark: SparkSession = SparkSession.builder.master("local[*]").appName("DroneReportConsumerSpark").getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", sys.env("AWS_ACCESS_KEY"))
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sys.env("AWS_SECRET_KEY"))

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "reportGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("droneData")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      rdd.map(record => Json.parse(record.value())).foreachPartition { iter =>
        val droneData = iter.flatMap { json =>
          val timestamp = (json \ "timestamp").as[String]
          val drone = (json \ "drone").as[JsObject]
          val droneId = (drone \ "id").as[Int]
          val droneLocation = (drone \ "location").as[Seq[Int]]
          Some((timestamp, droneId, droneLocation(0), droneLocation(1)))
        }.toSeq.toDF("timestamp", "drone_id", "drone_x", "drone_y")

        val citizenData = iter.flatMap { json =>
          val timestamp = (json \ "timestamp").as[String]
          val drone = (json \ "drone").as[JsObject]
          val droneId = (drone \ "id").as[Int]
          val droneLocation = (drone \ "location").as[Seq[Int]]
          val citizens = (json \ "citizens").asOpt[Seq[JsObject]].getOrElse(Seq.empty[JsObject])
          citizens.map { citizen =>
            val name = (citizen \ "name").as[String]
            val harmonyScore = (citizen \ "harmonyScore").as[Double]
            val words = (citizen \ "words").as[Seq[String]]
            (timestamp, droneId, droneLocation(0), droneLocation(1), name, harmonyScore, words.mkString(","))
          }
        }.toSeq.toDF("timestamp", "drone_id", "drone_x", "drone_y", "citizen_name", "harmony_score", "words")

        droneData.write.mode(SaveMode.Append).csv("s3a://inde-bucket/droneData")
        citizenData.write.mode(SaveMode.Append).csv("s3a://inde-bucket/citizenData")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
