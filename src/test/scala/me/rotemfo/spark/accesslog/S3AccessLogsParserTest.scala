package me.rotemfo.spark.accesslog

import me.rotemfo.spark.accesslog.processor.S3AccessLogProcessor.{EmptyS3Record, parseLogLine}
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.io.Source

class S3AccessLogsParserTest
  extends WordSpecLike
    with Matchers
    with BeforeAndAfterEach {

  "S3AccessLogsParserTest " should {
    "parse files correctly" in {
      val is = getClass.getResourceAsStream("/test-le-logs/emr2021-11-08-13-10-55-E99D7343D993A492")
      val source = Source.fromInputStream(is)
      val lines = source.getLines().toList
      source.close()
      is.close()
      val records = lines.map(parseLogLine).filter(r => !r.equals(EmptyS3Record()))
      assert(records.length == 21L)
      val map = records.groupBy(_.bucketName)
      assert(map.keys.head == "seekingalpha-data")
    }
  }
}
