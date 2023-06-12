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
  def checkIfExists(spark: SparkSession, path: String) = {
    val index = "s3://".length() + path.substring("s3://".length()).indexOf("/")
    val fs = FileSystem.get(new URI(path.substring(0, index)), spark.sparkContext.hadoopConfiguration)
  
    // check if already exists
    fs.exists(new Path(path + "/_SUCCESS")) 
  }

  
  def convertNullToEmptyString(str: String): String = {
    if (str == null) "" else str
  }

}
