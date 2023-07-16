package omigo.matel
 
import collection.JavaConverters._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.URI
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.BitSet
import org.apache.hadoop.fs._
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.ArrayBuffer 
import scala.util.Random

object Utils {
  val XUUIDS = "0123456789abcdef"

  def checkIfExists(spark: SparkSession, path: String) = {
    val index = "s3://".length() + path.substring("s3://".length()).indexOf("/")
    val fs = FileSystem.get(new URI(path.substring(0, index)), spark.sparkContext.hadoopConfiguration)
  
    // check if already exists
    fs.exists(new Path(path + "/_SUCCESS")) 
  }

  def convertNullToEmptyString(str: String): String = {
    if (str == null) "" else str
  }

  def listHadoopFilesAndDirectories(spark: SparkSession, path: String) = {
    val index = "s3://".length() + path.substring("s3://".length()).indexOf("/")
    val fs = FileSystem.get(new URI(path.substring(0, index)), spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(path)))
      fs.listStatus(new Path(path)).toList
    else
      List.empty
  }

  def listHadoopDirectories(spark: SparkSession, path: String) = {
    listHadoopFilesAndDirectories(spark, path).filter({ fileStatus => fileStatus.isDirectory() })
  }

  def listHadoopFiles(spark: SparkSession, path: String) = {
    listHadoopFilesAndDirectories(spark, path).filter({ fileStatus => fileStatus.isFile() })
  }

  def createSuccessFile(spark: SparkSession, path: String): Unit = {
    val index = "s3://".length() + path.substring("s3://".length()).indexOf("/")
    val fs = FileSystem.get(new URI(path.substring(0, index)), spark.sparkContext.hadoopConfiguration)
    // fs.mkdirs(new Path(path))
    fs.create(new Path(path + "/_SUCCESS")).close()
  }

  
  def urlEncode(value: String) = {
    if (value != null) java.net.URLEncoder.encode(value, "utf-8") else null
  }
  
  def urlDecode(value: String) = {
    if (value != null) java.net.URLDecoder.decode(value, "utf-8") else null
  }

  def readStringValue(row: Row, fieldName: String) = {
    val index = row.fieldIndex(fieldName)
    if (row.isNullAt(index) == false)
      row.getString(index)
    else
      ""
  }

  def readIntValue(row: Row, fieldName: String) = {
    val index = row.fieldIndex(fieldName)
    if (row.isNullAt(index) == false)
      row.getInt(index)
    else
      0
  }

  def readLongValue(row: Row, fieldName: String) = {
    val index = row.fieldIndex(fieldName)
    if (row.isNullAt(index) == false)
      row.getLong(index)
    else
      0L
  }

  def readMapValue(row: Row, fieldName: String) = {
    val index = row.fieldIndex(fieldName)
    if (row.isNullAt(index) == false)
      row.getMap(index)
    else
      Map.empty
  }
}

