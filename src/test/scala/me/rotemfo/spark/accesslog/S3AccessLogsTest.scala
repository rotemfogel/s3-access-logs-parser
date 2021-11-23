package me.rotemfo.spark.accesslog

import me.rotemfo.spark.accesslog.schema.S3AccessLogs.{dateCol, hourCol, s3AccessLogsSchema}
import me.rotemfo.spark.common.BaseTest
import org.apache.commons.io.FileUtils

import java.io.File
import java.time.LocalDateTime

class S3AccessLogsTest extends BaseTest {
  private val tableName = "test-le-logs"
  private val resourcesPath: String = s"${baseInputPath}resources/$tableName"
  private val baseOutputPath: String = getResourcePath(classOf[S3AccessLogsTest], "output/")
  private val p: S3AccessLogsConfig = S3AccessLogsConfig(
    inputBucket = Some(resourcesPath),
    prefix = Some("emr"),
    inputDateTime = Some(LocalDateTime.of(2021, 11, 8, 13, 0, 0)),
    outputPath = Some(s"${baseOutputPath}/$tableName"),
    partitions = 1
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteDirectory(new File(baseOutputPath))
    S3AccessLogsApp.doWork(p, spark.sqlContext)
  }

  trait OutputData {
    val df = spark.read.parquet(p.outputPath.get)
  }

  "S3AccessLogsTest " should {
    "process files correctly" in new OutputData {
      assert(df.count(), 1069L)
    }
    "test output schema, ignoring order" in new OutputData {
      assert(s3AccessLogsSchema.fieldNames.sorted, df.drop(dateCol, hourCol).schema.fieldNames.sorted)
    }
  }
}
