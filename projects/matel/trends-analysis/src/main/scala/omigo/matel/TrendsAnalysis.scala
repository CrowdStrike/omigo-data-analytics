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
      .config("spark.yarn.maxAppAttempts", "1")
      .getOrCreate()

    // run different steps

    // 1. read input data from lokki library and write it back in parquet
    readBaseData(spark, inputFile, outputDir + "/base", props)

    // 2. run custom enrichment step
    runEnrichment(spark, outputDir + "/base", outputDir + "/enrichment")

    // 3. create ancestry
    val numLevels = 6
    createHierarchy(spark, outputDir + "/enrichment", numLevels, outputDir + "/ancestry")

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
    val cols = Seq("id_level1", "id_level2", "uid", "node_id", "node_label", "parent_id", "event_name", "event_id", "ts")
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
    if (false && Utils.checkIfExists(spark, outputPath) == true) {
      println("doEnrichment: outputPath already exists. Skipping ... : " + outputPath)
      return
    }

    val base = spark.read.parquet(inputPath)
    base.createOrReplaceTempView("base")

    // base kv
    val baseKV = spark.sql("select id_level1, id_level2, uid, node_id, node_label, parent_id, event_name, event_id, ts from base").rdd.map({ row =>
      val id_level1 = row.getString(row.fieldIndex("id_level1"))
      val id_level2 = row.getString(row.fieldIndex("id_level2"))
      val uid = row.getString(row.fieldIndex("uid"))
      val node_id = row.getString(row.fieldIndex("node_id"))
      val node_label = row.getString(row.fieldIndex("node_label"))
      val parent_id = row.getString(row.fieldIndex("parent_id"))
      val event_name = row.getString(row.fieldIndex("event_name"))
      val event_id = row.getString(row.fieldIndex("event_id"))
      val ts = row.getString(row.fieldIndex("ts"))

      ((id_level1, id_level2, event_id, ts), (uid, node_id, node_label, parent_id, event_name))
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
      case ((id_level1, id_level2, event_id, ts), ((uid, node_id, node_label, parent_id, event_name), (localDateTimeNum, localHour, localDayOfWeek))) =>
        val keyValues = List(
          ("local_date_time_num", localDateTimeNum.toString),
          ("local_hour", localHour.toString),
          ("local_day_of_week", localDayOfWeek.toString)
        )
        .toMap
        (id_level1, id_level2, uid, node_id, node_label, parent_id, event_name, event_id, ts, keyValues)
    })

    // create dataframe with keyvalues
    val cols = Seq("id_level1", "id_level2", "uid", "node_id", "node_label", "parent_id", "event_name", "event_id", "ts", "kv")
    val df = spark.createDataFrame(joined).toDF(cols:_*)

    // persist
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
    spark.catalog.dropTempView("base")
  }

  /**
   * Method to generate the node hierarchy
   */
  def createHierarchy(spark: SparkSession, inputPath: String, numLevels: Int, outputPath: String): Unit = {
    import spark.implicits._
    import spark.sqlContext.implicits._

    // check if already exists
    if (false && Utils.checkIfExists(spark, outputPath) == true) {
      println("createHierarchy: outputPath already exists. Skipping ... : " + outputPath)
      return
    }

    // read base 
    val base = spark.read.parquet(inputPath)
    base.createOrReplaceTempView("base")

    // base kv
    val baseRDD = spark.sql("select id_level1, id_level2, node_id, parent_id, node_label, event_id, ts from base").rdd.map({ row =>
      val id_level1 = row.getString(row.fieldIndex("id_level1"))
      val id_level2 = row.getString(row.fieldIndex("id_level2"))
      val node_id = row.getString(row.fieldIndex("node_id"))
      val parent_id = row.getString(row.fieldIndex("parent_id"))
      val node_label = row.getString(row.fieldIndex("node_label"))
      val event_id = row.getString(row.fieldIndex("event_id"))
      val ts = row.getString(row.fieldIndex("ts"))

      // generate only for valid node ids
      (id_level1, id_level2, node_id, parent_id, node_label, event_id, ts)
    })

    // parent
    val parentInfo = baseRDD
      .filter({ case (id_level1, id_level2, node_id, parent_id, node_label, event_id, ts) => node_id != "" })
      .map({ case (id_level1, id_level2, node_id, parent_id, node_label, event_id, ts) =>
        (id_level1, id_level2, node_id, parent_id, node_label, ts)
      })

    // ancestor level0
    val ancestorLevelBase = baseRDD.map({
      case (id_level1, id_level2, node_id, parent_id, node_label, event_id, ts) =>
        (id_level1, id_level2, event_id, parent_id, ts, List(node_id), List(node_label))
    })

    // run loop to generate ancestry upto given levels
    var ancestorLevel = ancestorLevelBase
    Range(0, numLevels).foreach({ n =>
      println("createHierarchy: running a round for ancestry: %s".format(n))
      ancestorLevel = addParent(ancestorLevel, parentInfo)
    })

    // base kv
    val baseKV = spark.sql("select * from base").rdd.map({ row =>
      val id_level1 = row.getString(row.fieldIndex("id_level1"))
      val id_level2 = row.getString(row.fieldIndex("id_level2"))
      val uid = row.getString(row.fieldIndex("uid"))
      val node_id = row.getString(row.fieldIndex("node_id"))
      val parent_id = row.getString(row.fieldIndex("parent_id"))
      val node_label = row.getString(row.fieldIndex("node_label"))
      val event_name = row.getString(row.fieldIndex("event_name"))
      val event_id = row.getString(row.fieldIndex("event_id"))
      val ts = row.getString(row.fieldIndex("ts"))
      val kv = row.getMap(row.fieldIndex("kv")).asInstanceOf[Map[String, String]]

      ((id_level1, id_level2, event_id, ts), (uid, node_id, parent_id, node_label, event_name, kv))
    })

    // take final ancestor level
    val ancestorLevelKV = ancestorLevel
      .map({ case (id_level1, id_level2, event_id, level_ppid, ts, ppid_list, ppid_label_list) =>
        ((id_level1, id_level2, event_id, ts), (ppid_list, ppid_label_list))
      })

    // join
    val joined = (baseKV leftOuterJoin ancestorLevelKV)
      .map({ case ((id_level1, id_level2, event_id, ts), ((uid, node_id, parent_id, node_label, event_name, kv), ancestorOpt)) =>
        val (ppid_list, ppid_label_list) = ancestorOpt.getOrElse((List.empty, List.empty))

        // append the new columns to kv
        val ancestry_kv = List(
          ("ancestor_ids", ppid_list.filter(_ != "").mkString(",")),
          ("ancestor_labels", ppid_label_list.filter(_ != "").mkString(",")),
          ("ancestor_count", ppid_list.length.toString)
        ).toMap

        (id_level1, id_level2, uid, node_id, parent_id, node_label, event_name, event_id, ts, kv, ancestry_kv)
      })

    // create dataframe
    val cols = List("id_level1", "id_level2", "uid", "node_id", "parent_id", "node_label", "event_name", "event_id", "ts", "kv", "ancestry_kv")
    val df = spark.createDataFrame(joined).toDF(cols:_*)

    // persist
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
    spark.catalog.dropTempView("base")
  }

  /**
   * Method to add one level parent informaton. TODO: The timestamp handling can have side effect, and need proper config parameters
   */
  def addParent(nodeLevel: RDD[(String, String, String, String, String, List[String], List[String])], parentInfo: RDD[(String, String, String, String, String, String)], numReducers: Int = 10) = {
    // create event level kv
    val nodeLevelKV = nodeLevel.map({
      case (id_level1, id_level2, level_ppid, event_id, ts, ppid_list, ppid_label_list) =>
        ((id_level1, id_level2, level_ppid), (event_id, ts, ppid_list, ppid_label_list))
    })

    // create event parent. TODO custom filtering function
    val parentInfoKV = parentInfo
      .map({ case (id_level1, id_level2, node_id, parent_id, node_label, ts) =>
        ((id_level1, id_level2, node_id), (parent_id, node_label, ts))
      })
      // .filter({ case ((id_level1, id_level2, node_id), (parent_id, node_label, ts)) => node_id != null && node_id != ""}) // The parents are already non null
      .groupByKey(numReducers)

    // join
    val joined = (nodeLevelKV leftOuterJoin parentInfoKV)
      .map({
        case ((id_level1, id_level2, level_ppid), ((event_id, ts, ppid_list, ppid_label_list), vsParentOpt)) =>
          val vsParent = vsParentOpt.getOrElse(List.empty)
          val vsParentBefore = vsParent.filter({ case (p_ppid, p_node_label, p_ts) =>
            // TODO: parent should be before child. is there noise. Custom filter on invalid parent ids
            ppid_list.contains(p_ppid) == false // && (p_ppid != null && p_ppid != "")
          })

          // incorporate timestamp filters if present
          val vsParentBeforeTS = vsParentBefore.filter({ case (p_ppid, p_node_label, p_ts) => (ts == "" || p_ts == "" || p_ts.toLong <= ts.toLong) })

          // pick the most recent parent
          if (vsParentBeforeTS.nonEmpty) {
            // get the most relevant parent based on timestamp or the string sorting of ppid
            val (p_ppid, p_node_label, p_ts) = {
              if (ts != "")
                vsParentBeforeTS.maxBy({ case (p_ppid2, p_node_label2, p_ts2) => p_ts2.toLong })
              else
                vsParentBeforeTS.maxBy({ case (p_ppid2, p_node_label2, p_ts2) => p_ppid2 })
            }

            // ts is the timestamp of the event
            (id_level1, id_level2, event_id, p_ppid, ts, ppid_list ++ List(level_ppid), ppid_label_list ++ List(p_node_label))
          } else {
            (id_level1, id_level2, event_id, level_ppid, ts, ppid_list, ppid_label_list)
          }
      })

    joined
  }

  def createGroups(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

  }

  def createDicts(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

  }
}
