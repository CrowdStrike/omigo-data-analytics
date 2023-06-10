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
    val cols = Seq("id_level1", "id_level2", "uid", "node_id", "node_name", "parent_id", "event_name", "event_id", "ts")
    println(cols)

    val tuples = xtsv.to_tuples9(cols, "readBaseData")
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

    // base kv
    val baseKV = spark.sql("select id_level1, id_level2, uid, node_id, node_name, parent_id, event_name, event_id, ts from base").rdd.map({ row =>
      val id_level1 = row.getString(row.fieldIndex("id_level1"))
      val id_level2 = row.getString(row.fieldIndex("id_level2"))
      val uid = row.getString(row.fieldIndex("uid"))
      val node_id = row.getString(row.fieldIndex("node_id"))
      val node_name = row.getString(row.fieldIndex("node_name"))
      val parent_id = row.getString(row.fieldIndex("parent_id"))
      val event_name = row.getString(row.fieldIndex("event_name"))
      val event_id = row.getString(row.fieldIndex("event_id"))
      val ts = row.getString(row.fieldIndex("ts"))

      ((id_level1, id_level2, event_id, ts), (uid, node_id, node_name, parent_id, event_name))
    })

    // read data
    val dayOfWeekKV = spark.sql("select id_level1, id_level2, event_id, ts from base").rdd.map({ row =>
      // read data
      val id_level1 = row.getString(row.fieldIndex("id_level1"))
      val id_level2 = row.getString(row.fieldIndex("id_level2"))
      val event_id = row.getString(row.fieldIndex("event_id"))
      val ts = row.getString(row.fieldIndex("ts"))

      // FIXME: workaround to parse the string
      val localTimeEpochMS = ((ts.toLong - 116444736000000000L) / 10000.0).toLong

      // FIXME: Timstamo format change is weird
      val localDateTime = Instant.ofEpochMilli(localTimeEpochMS).atZone(ZoneId.systemDefault()).toLocalDateTime()
      val localDateTimeNum = "%04d%02d%02d%02d".format(localDateTime.getYear, localDateTime.getMonthValue, localDateTime.getDayOfMonth, localDateTime.getHour).toLong
      val localHour = localDateTime.getHour
      val localDayOfWeek = localDateTime.getDayOfWeek.getValue()

      ((id_level1, id_level2, event_id, ts), (localDateTimeNum, localHour, localDayOfWeek))
    })

    // join
    val joined = (baseKV join dayOfWeekKV).map({
      case ((id_level1, id_level2, event_id, ts), ((uid, node_id, node_name, parent_id, event_name), (localDateTimeNum, localHour, localDayOfWeek))) =>
        val keyValues = List(
          ("local_date_time_num", localDateTimeNum.toString),
          ("local_hour", localHour.toString),
          ("local_day_of_week", localDayOfWeek.toString)
        )
        .toMap
        (id_level1, id_level2, uid, node_id, node_name, parent_id, event_name, event_id, ts, keyValues)
    })

    // create dataframe with keyvalues
    val cols = Seq("id_level1", "id_level2", "uid", "node_id", "node_name", "parent_id", "event_name", "event_id", "ts", "keyValues")
    val df = spark.createDataFrame(joined).toDF(cols:_*)

    // persist
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
    spark.catalog.dropTempView("base")
  }

  def runAncestry(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    import spark.implicits._
    import spark.sqlContext.implicits._

    // check if already exists
    if (Utils.checkIfExists(spark, outputPath) == true) {
      println("runAncestry: outputPath already exists. Skipping ... : " + outputPath)
      return
    }

    // read base 
    val base = spark.read.parquet(inputPath)
    base.createOrReplaceTempView("base")

    // base kv
    val baseRDD = spark.sql("select id_level1, id_level2, node_id, node_name, parent_id, ts from base").rdd.map({ row =>
      val id_level1 = row.getString(row.fieldIndex("id_level1"))
      val id_level2 = row.getString(row.fieldIndex("id_level2"))
      val node_id = row.getString(row.fieldIndex("node_id"))
      val node_name = row.getString(row.fieldIndex("node_name"))
      val parent_id = row.getString(row.fieldIndex("parent_id"))
      val ts = row.getString(row.fieldIndex("ts"))

      // generate only for valid node ids
      (id_level1, id_level2, node_id, parent_id, ts.toLong)
    })

    // parent
    val parentInfo = baseRDD.filter({ case (id_level1, id_level2, node_id, parent_id, ts) => node_id != "" })

    // children
    val emptyList: List[String] = List.empty

    // ancestor level0
    val ancestorLevel0 = parentInfo.map({ case (id_level1, id_level2, node_id, parent_id, ts) => (id_level1, id_level2, parent_id, event_id, ts, List(node_id)) })
    val ancestorLevel1 = addOneLevelParent(ancestorLevel0, parentInfo)
    val ancestorLevel2 = addOneLevelParent(ancestorLevel1, parentInfo)
    val ancestorLevel3 = addOneLevelParent(ancestorLevel2, parentInfo)
    val ancestorLevelFinal = ancestorLevel3

    // base kv
    val baseKV = spark.sql("select id_level1, id_level2, uid, node_id, parent_id, event_name, event_id, ts from base").rdd.map({ row =>
      val id_level1 = row.getString(row.fieldIndex("id_level1"))
      val id_level2 = row.getString(row.fieldIndex("id_level2"))
      val uid = row.getString(row.fieldIndex("uid"))
      val node_id = row.getString(row.fieldIndex("node_id"))
      val parent_id = row.getString(row.fieldIndex("parent_id"))
      val event_name = row.getString(row.fieldIndex("event_name"))
      val event_id = row.getString(row.fieldIndex("event_id"))
      val ts = row.getString(row.fieldIndex("ts"))

      ((id_level1, id_level2, event_id, ts), (uid, node_id, parent_id, event_name))
    })

    // take final ancestor level
    val ancestorLevelKV = ancestorLevelFinal
      .map({ case (id_level1, id_level2, event_id, level_ppid, ts, ppid_list) =>
        ((id_level1, id_level2, event_id, ts), ppid_list)
      })

    val joined = (baseKV leftOuterJoin ancestorLevelKV)
      .map({ case ((id_level1, id_level2, event_id, ts), ((uid, node_id, parent_id, event_name), ancestorOpt)) =>
        val ppid_list = ancestorOpt.getOrElse(List.empty)
      })
  }

  /*
   * Method to add one level parent informaton.
   */
  def addOneLevelParent(eventLevel: RDD[(String, String, String, String, Long, List[String])], parentInfo: RDD[(String, String, String, String, Long)]) = {
    val eventLevelKV = eventLevel.map({ case (id_level1, id_level2, level_ppid, event_id, ts, ppid_list) => ((id_level1, id_level2, level_ppid), (event_id, ts, ppid_list)) })

    val eventParentInfoKV = parentInfo
      .map({ case (id_level1, id_level2, node_id, parent_id, ts) => ((id_level1, id_level2, node_id), (parent_id, ts)) })
      .filter({ case ((id_level1, id_level2, node_id), (parent_id, ts)) => node_id != null && node_id != "" && node_id != "0"}) // The parents are already non null
      .groupByKey(1000)

    (eventLevelKV leftOuterJoin eventParentInfoKV)
      .map({
        case ((id_level1, id_level2, level_ppid), ((event_id, ts, ppid_list), vsParentOpt)) =>
          val vsParent = vsParentOpt.getOrElse(List.empty)
          val vsParentBefore = vsParent.filter({ case (p_ppid, p_ts) =>
            ppid_list.contains(p_ppid) == false && (p_ppid != null && p_ppid != "" && p_ppid != "0") &&
            (p_ts <= ts) // TODO: parent should be before child. is there noise?. Linux recycles pids frequently may be
          })

          // pick the most recent parent
          if (vsParentBefore.nonEmpty) {
            val vParentMax = vsParentBefore.maxBy({ case (p_ppid, p_ts) => p_ts})
            val (p_ppid, p_ts) = vParentMax

            // ts is the timestamp of the event
            (id_level1, id_level2, event_id, p_ppid, ts, ppid_list ++ List(level_ppid))
          } else {
            (id_level1, id_level2, event_id, level_ppid, ts, ppid_list)
          }
      })
  }

  def createGroups(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

  }

  def createDicts(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

  }
}
