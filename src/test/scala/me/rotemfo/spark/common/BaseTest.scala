package me.rotemfo.spark.common

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpecLike}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File

trait BaseTest extends WordSpecLike
  with Matchers
  with BeforeAndAfterEach
  with DataFrameSuiteBase {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)
  private final val os = System.getProperty("os.name").toLowerCase()

  override implicit def enableHiveSupport: Boolean = false

  final val sep = "/"

  private val path: Array[String] = getClass.getResource(sep).getPath.split(sep).dropRight(3)
  private val testPath: String = (path ++ Array("src", "test")).mkString(sep).concat(sep)

  protected val baseInputPath: String = {
    getOsSpecificPath(testPath)
  }

  /**
   * remove leading "/" from windows path (e.g. /C:/code/....)
   *
   * @param path the OS path as Array
   * @return
   */
  protected def getOsSpecificPath(path: Array[String]): String = getOsSpecificPath(path.mkString(sep))

  /**
   * remove leading "/" from windows path (e.g. /C:/code/....)
   *
   * @param path the OS path
   * @return
   */
  protected def getOsSpecificPath(path: String): String = {
    val p = if (os.indexOf("win") >= 0 && path.startsWith(sep)) path.substring(1) else path
    if (p.endsWith(sep)) p else p.concat(sep)
  }

  protected def getResourcePath[T](classOfT: Class[T], resource: String): String = {
    val root: String = classOfT.getClassLoader.getResource(".").getPath
    val directory: File = new File(root + File.separator + resource)
    if (!directory.exists()) {
      directory.mkdir()
    }
    // If you require it to make the entire directory path including parents,
    // use directory.mkdirs(); here instead.
    classOfT.getClassLoader.getResource(resource).getPath
  }
}
