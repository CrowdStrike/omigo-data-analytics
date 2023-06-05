package omigo.matel

import collection.JavaConverters._
import java.io.FileInputStream
import java.net.URI
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Properties
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder,Encoders}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf, SparkFiles}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import scala.reflect.ClassTag

object TrendsAnalysis {
  def main(args: Array[String]): Unit = {
    // Check command line parameters
    if (args.length != 3) {
        println("Usage: TrendsAnalysis <config-file> <input-dir> <output-dir>")
        return
    }

    // read command line parameters
    val propsFile = args(0)
    val inputDir = args(1)
    val outputDir = args(2)

    // Debug
    println("Parameter: propsFile: " + propsFile)
    println("Parameter: inputDir:  " + inputDir)
    println("Parameter: outputDir: " + outputDir)

    // read configuration file
    val props = new Properties()
    props.load(new FileInputStream(propsFile))

    // Debug
    println("Properties: " + props)
  }
}
