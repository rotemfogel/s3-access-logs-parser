package me.rotemfo.spark.accesslog.processor

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.util.matching.Regex

object S3AccessLogProcessor {
  case class S3Record(bucketOwner: String,
                      bucketName: String,
                      requestDateTime: Timestamp,
                      remoteIp: String,
                      requester: String,
                      requestId: String,
                      operation: String,
                      key: String,
                      requestUri: String,
                      httpStatus: String,
                      errorCode: String,
                      bytesSent: Option[Long],
                      objectSize: Option[Long],
                      totalTime: String,
                      turnAroundTime: String,
                      referrer: String,
                      userAgent: String,
                      versionId: String,
                      hostId: Option[String],
                      signatureVersion: Option[String],
                      cipherSuite: Option[String],
                      authType: Option[String],
                      hostHeader: Option[String],
                      tlsVersion: Option[String])

  object EmptyS3Record {
    def apply(): S3Record = S3Record(null, null, null, null, null, null,
      null, null, null, null, null, None,
      None, null, null, null, null, null,
      None, None, None, None, None, None)
  }

  private lazy val regex: Regex = """(\S+)\s(\S+)\s\[([^\]]+)\]\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(-|"-"|"\S+ \S+ (?:-|\S+)")\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s(-|"[^"]+")\s(-|"[^"]+")\s(\S+)(.*)""".r

  private lazy val logDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss +0000")
  private lazy val quote: Char = '\"'

  private def dropQuotes(s: String): String = {
    if (s.equals("-")) s
    else {
      if (s.head.equals(quote) && s.last.equals(quote)) s.substring(1, s.length - 1)
      else s
    }
  }

  def parseLogLine(s: String): S3Record = {
    s match {
      case regex(bucketOwner, bucketName, requestDateTime, remoteIp, requester, requestId, operation,
      key, requestUri, httpStatus, errorCode, bytesSent, objectSize, totalTime, turnAroundTime, referrer,
      userAgent, versionId, additional) =>
        val date = Timestamp.valueOf(LocalDateTime.parse(requestDateTime, logDateFormatter))
        val additionalData = additional.split("\\s")
        val hostId = if (additionalData.nonEmpty) Some(additionalData(0)) else None
        val signatureVersion = if (additionalData.length > 1) Some(additionalData(1)) else None
        val cipherSuite = if (additionalData.length > 2) Some(additionalData(2)) else None
        val authType = if (additionalData.length > 3) Some(additionalData(3)) else None
        val hostHeader = if (additionalData.length > 4) Some(additionalData(4)) else None
        val tlsVersion = if (additionalData.length > 5) Some(additionalData(5)) else None

        val nBytesSent = Try(Some(bytesSent.toLong)).getOrElse(None)
        val nObjectSize = Try(Some(objectSize.toLong)).getOrElse(None)

        S3Record(bucketOwner, bucketName, date, remoteIp, requester, requestId, operation,
          key, dropQuotes(requestUri), httpStatus, errorCode, nBytesSent, nObjectSize, totalTime, turnAroundTime,
          dropQuotes(referrer), dropQuotes(userAgent), versionId, hostId, signatureVersion, cipherSuite, authType,
          hostHeader, tlsVersion)
      case _ => EmptyS3Record()
    }
  }
}
