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

import omigo_core.TSV

object TrendsAnalysis {
  def main(args: Array[String]): Unit = {
    // Check command line parameters
    if (args.length != 3) {
        println("Usage: TrendsAnalysis <config-file> <input-file> <output-dir>")
        return
    }

    // read command line parameters
    val propsFile = args(0)
    val inputFile = args(1)
    val outputDir = args(2)

    // Debug
    println("Parameter: propsFile: " + propsFile)
    println("Parameter: inputFile:  " + inputFile)
    println("Parameter: outputDir: " + outputDir)

    // read configuration file
    val props = new Properties()
    props.load(new FileInputStream(propsFile))

    // Debug
    println("Properties: " + props)

    // initialize spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .appName("TrendsAnalysis")
      .getOrCreate()

    // run different steps

    // 1. read input data from lokki library and write it back in parquet
    readBaseData(spark, inputFile, outputDir + "/base", props)

    // 2. run custom enrichment step
    runEnrichment(spark, outputDir + "/base", outputDir + "/enrichment")

    // 3. create ancestry
    runAncestry(spark, outputDir + "/enrichment", outputDir + "/ancestry")

    // 4. group events
    createGroups(spark, outputDir + "/ancestry", outputDir + "/groups")

    // 5. generate different kinds of dictionaries
    createDicts(spark, outputDir + "/groups", outputDir + "/dicts")
  }

  def readBaseData(spark: SparkSession, inputFile: String, outputPath: String, props: Properties) = {
    import spark.implicits._
    import spark.sqlContext.implicits._

    // read the tsv file
    val xtsv = TSV.read(inputFile, "\t")

    // read the data
    val cols = Seq("id_level1", "id_level2", "uid", "node_id", "parent_id", "event_id")
    println(cols)

    val tuples = xtsv.to_tuples6(cols, "readBaseData")
    // println(tuples)

    // create parquet file with all necessary columns
    val df = spark.createDataFrame(spark.sparkContext.parallelize(tuples)).toDF(cols:_*)

    // persist 
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def runEnrichment(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    import spark.implicits._
    import spark.sqlContext.implicits._

    // check if already exists
    if (Utils.checkIfExists(spark, outputPath) == true) {
      println("doEnrichment: outputPath already exists. Skipping ... : " + outputPath)
      return
    }

    val base = spark.read.parquet(inputPath)
    base.createOrReplaceTempView("base")

    // read data
    val df = spark.sql("select * from base")

    // persist
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
    spark.catalog.dropTempView("base")
  }

  def runAncestry(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

  }

  def createGroups(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

  }

  def createDicts(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

  }
}
