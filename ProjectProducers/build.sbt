name := "ProjectProducers"

version := "1.0"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.4.1",
  "org.apache.kafka" % "kafka_2.13" % "2.4.1",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "software.amazon.awssdk" % "s3" % "2.17.82"

)

libraryDependencies ++= {
    val sparkVersion = "3.4.1" // Vérifiez que vous utilisez la bonne version de Spark ici
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion, // pour Kafka
    )
    }
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.375"