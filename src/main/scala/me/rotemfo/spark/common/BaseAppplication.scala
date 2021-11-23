package me.rotemfo.spark.common

import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import scala.util.{Failure, Try}

abstract class BaseApplication[P <: Product](p: P) extends SparkApp[P] {
  def main(args: Array[String]): Unit = {
    getParser.parse(args, p).foreach { p =>
      logger.info(p.toString)

      val sparkSessionBuilder = SparkSession.builder()
        .enableHiveSupport()
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.broadcastTimeout", 6600)
        .config("spark.sql.shuffle.partitions", "200")

      val sparkSession = sparkSessionBuilder.getOrCreate
      val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
      hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
      hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")

      val processResults: Try[Unit] = Try(doWork(p, sparkSession.sqlContext))
      sparkSession.sparkContext.getPersistentRDDs.values.foreach(_.unpersist())
      sparkSession.close()
      processResults match {
        case Failure(ex) => throw ex
        case _ =>
      }
    }
  }
}

object BaseApplication {
  val dateHourPattern = "yyyy-MM-dd-HH"
  val dateHourPatternFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateHourPattern)
}