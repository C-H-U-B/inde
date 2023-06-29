import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, ListObjectsV2Request}
import com.github.tototoshi.csv._
import java.io.{ByteArrayInputStream, InputStreamReader, BufferedReader}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer

object CSVConcatenator {
  def main(args: Array[String]): Unit = {
    val accessKey = "AKIAUTVLSGKXSLAEH26B"
    val secretKey = "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"

    val region = Region.EU_WEST_1
    val bucketName = "inde-bucket"
    val prefix = "drone_report_"

    val credentials = AwsBasicCredentials.create(accessKey, secretKey)

    val s3 = S3Client.builder()
      .region(region)
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .build()

    val req = ListObjectsV2Request.builder()
      .bucket(bucketName)
      .prefix(prefix)
      .build()

    val result = s3.listObjectsV2(req)

    val fileKeys = result.contents().asScala.map(_.key()).toList

    val allData = new ListBuffer[Seq[String]]

    fileKeys.foreach { key =>
      val getRequest = GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()

      val objectContent = s3.getObjectAsBytes(getRequest).asByteArray()
      val data = CSVReader.open(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(objectContent)))).all()
      allData ++= data
    }

    val outputFilePath = "output.csv"
    val writer = CSVWriter.open(outputFilePath)
    writer.writeAll(allData.toList)
    writer.close()
  }
}
