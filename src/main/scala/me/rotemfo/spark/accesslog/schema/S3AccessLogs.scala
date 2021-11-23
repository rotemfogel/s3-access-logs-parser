package me.rotemfo.spark.accesslog.schema

import org.apache.spark.sql.types._

object S3AccessLogs {

  // @see https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-s3-access-logs-to-identify-requests.html#querying-s3-access-logs-for-requests
  lazy val bucketOwnerCol: String = "bucketOwner"
  lazy val bucketNameCol: String = "bucketName"
  lazy val requestDateTimeCol: String = "requestDateTime"
  lazy val remoteIpCol: String = "remoteIp"
  lazy val requesterCol: String = "requester"
  lazy val requestIdCol: String = "requestId"
  lazy val operationCol: String = "operation"
  lazy val keyCol: String = "key"
  lazy val requestUriCol: String = "requestUri"
  lazy val httpStatusCol: String = "httpStatus"
  lazy val errorCodeCol: String = "errorCode"
  lazy val bytesSentCol: String = "bytesSent"
  lazy val objectSizeCol: String = "objectSize"
  lazy val totalTimeCol: String = "totalTime"
  lazy val turnAroundTimeCol: String = "turnAroundTime"
  lazy val referrerCol: String = "referrer"
  lazy val userAgentCol: String = "userAgent"
  lazy val versionIdCol: String = "versionId"
  lazy val hostIdCol: String = "hostId"
  lazy val signatureVersionCol: String = "signatureVersion"
  lazy val cipherSuiteCol: String = "cipherSuite"
  lazy val authTypeCol: String = "authType"
  lazy val hostHeaderCol: String = "hostHeader"
  lazy val tlsVersionCol: String = "tlsVersion"

  lazy val dateCol: String = "date_"
  lazy val hourCol: String = "hour"

  lazy val logLineCol: String = "log_line"
  lazy val s3RecordJsonCol: String = "s3_record_json"

  val s3AccessLogsSchema: StructType = StructType(
    Seq(
      StructField(bucketOwnerCol, StringType, nullable = true),
      StructField(bucketNameCol, StringType, nullable = true),
      StructField(requestDateTimeCol, TimestampType, nullable = true),
      StructField(remoteIpCol, StringType, nullable = true),
      StructField(requesterCol, StringType, nullable = true),
      StructField(requestIdCol, StringType, nullable = true),
      StructField(operationCol, StringType, nullable = true),
      StructField(keyCol, StringType, nullable = true),
      StructField(requestUriCol, StringType, nullable = true),
      StructField(httpStatusCol, StringType, nullable = true),
      StructField(errorCodeCol, StringType, nullable = true),
      StructField(bytesSentCol, LongType, nullable = true),
      StructField(objectSizeCol, LongType, nullable = true),
      StructField(totalTimeCol, StringType, nullable = true),
      StructField(turnAroundTimeCol, StringType, nullable = true),
      StructField(referrerCol, StringType, nullable = true),
      StructField(userAgentCol, StringType, nullable = true),
      StructField(versionIdCol, StringType, nullable = true),
      StructField(hostIdCol, StringType, nullable = true),
      StructField(signatureVersionCol, StringType, nullable = true),
      StructField(cipherSuiteCol, StringType, nullable = true),
      StructField(authTypeCol, StringType, nullable = true),
      StructField(hostHeaderCol, StringType, nullable = true),
      StructField(tlsVersionCol, StringType, nullable = true)
    )
  )
}
