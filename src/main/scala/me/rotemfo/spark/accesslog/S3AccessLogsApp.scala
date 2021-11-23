package me.rotemfo.spark.accesslog

import me.rotemfo.spark.accesslog.processor.S3AccessLogProcessor.parseLogLine
import me.rotemfo.spark.accesslog.schema.S3AccessLogs._
import me.rotemfo.spark.common.BaseApplication
import me.rotemfo.spark.common.BaseApplication.dateHourPatternFormatter
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, SQLContext, SaveMode}
import scopt.OptionParser

import java.sql.Date
import java.time.LocalDateTime

case class S3AccessLogsConfig(inputPath: Option[String] = None,
                              prefix: Option[String] = Some(""),
                              inputDateTime: Option[LocalDateTime] = None,
                              outputPath: Option[String] = None,
                              partitions: Int = 4)

// @formatter:off
object S3AccessLogsParser extends OptionParser[S3AccessLogsConfig](programName = "S3AccessLogsApp Application") {
  opt[String]("input-path")     .required.action { (x, p) => p.copy(inputPath = Some(x)) }
  opt[String]("prefix")                  .action { (x, p) => p.copy(prefix = Some(x)) }
  opt[String]("input-datetime") .required.action { (x, p) => p.copy(inputDateTime = Some(LocalDateTime.parse(x, dateHourPatternFormatter))) }
  opt[String]("output-path")    .required.action { (x, p) => p.copy(outputPath = Some(x)) }
  opt[Int]("partitions")                 .action { (x, p) => p.copy(partitions = x) }
}
// @formatter:on

object S3AccessLogsApp extends BaseApplication[S3AccessLogsConfig](S3AccessLogsConfig()) {

  private def getJsonObjectNullSafe(c: Column, path: String): Column = {
    when(c.isNotNull, get_json_object(c, path))
      .otherwise(lit(null))
  }

  private val udfParseLogLine: UserDefinedFunction = udf((s: String) => parseLogLine(s))

  def doWork(implicit p: S3AccessLogsConfig, spark: SQLContext): Unit = {
    val dateString: String = p.inputDateTime.get.format(dateHourPatternFormatter)
    val inputBucket = if (p.inputPath.get.endsWith("/")) p.inputPath.get else s"${p.inputPath.get}/"
    val path = s"$inputBucket${p.prefix.get}$dateString*"

    val jsonDf = spark.read
      .schema(StructType(Seq(StructField(logLineCol, StringType, nullable = false))))
      .text(path)
      .withColumn(s3RecordJsonCol, to_json(udfParseLogLine(col(logLineCol))))
      .drop(logLineCol)

    val df = s3AccessLogsSchema.fields.foldLeft(jsonDf) {
      case (dataframe, column) => dataframe.withColumn(column.name, getJsonObjectNullSafe(col(s3RecordJsonCol), s"$$.${column.name}").cast(column.dataType))
    }.select(s3AccessLogsSchema.fieldNames.head, s3AccessLogsSchema.fieldNames.tail: _*)

    df.where(col(requestDateTimeCol).isNotNull)
      .withColumn(dateCol, lit(Date.valueOf(p.inputDateTime.get.toLocalDate)))
      .withColumn(hourCol, lit(p.inputDateTime.get.getHour))
      .coalesce(p.partitions)
      .write
      .partitionBy(dateCol, hourCol)
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "dynamic")
      .parquet(p.outputPath.get)
  }

  override protected def getParser: OptionParser[S3AccessLogsConfig] = S3AccessLogsParser
}