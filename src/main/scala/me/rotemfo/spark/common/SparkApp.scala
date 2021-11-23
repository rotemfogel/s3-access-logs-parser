package me.rotemfo.spark.common

import org.apache.spark.sql.SQLContext
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

trait SparkApp[P <: Product] {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  protected def doWork(implicit p: P, spark: SQLContext): Unit

  protected def getParser: OptionParser[P]
}

