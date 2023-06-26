package omigo.matel

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
import collection.JavaConverters._

import omigo_core.TSV

object TrendsAnalysis extends Serializable {
    val EmptyVal = """'a'"""
    val Empty1 = "empty1"
    val Empty2 = "empty2"
    val Empty3 = "empty3"

    // FIXME: All these constants go into config file
    val DefaultAnchorTs = 1577836800L // 2020-01-01
    val DefaultAnchorTsStr = "2020-01-01T00:00:00"

    val SelectedBinaryFeatures = Seq("Feature1", "Feature2")
    val ExcludedEventGroupNames = Seq("Feature3")

    // set the type of metric on the basis of strong trend or not
    val MinStrongTrendLearningMedian = 0
    val MinStrongTrendNumLearningSeq = 0

    val MinBinaryFeaturesCount = 0
    val MinStrongFeatureCount = 0
    val MinDictFeaturesCountPos = 0
    val MinDictFeaturesCountNeg = 0

    val MinWeakDiffFromMaxThreshRatio = 0
    val MinWeakDiffFromMaxThreshDefault = 0

    val MinWeakDiffFromMaxThreshMap = List(
      ("Feature1", 1),
      ("Feature2", 1)
    )
    .toMap

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
      val minusWindow = 3

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

      // group columns
      def eventMappingFunc(x: String) = x
      def kvMappingFunc(mp: Map[String, String]): Map[String, String] = mp
      createGroups(spark, outputDir + "/ancestry", eventMappingFunc, kvMappingFunc, outputDir + "/groups")

      // 5. generate different kinds of dictionaries
      createDicts(spark, outputDir + "/groups", outputDir + "/dicts")

      // 6. generate stats of different kinds
      generateStats(spark, outputDir + "/dicts", outputDir + "/stats")

      // 7. generate paired stats of different kinds
      generatePairedStats(spark, outputDir + "/dicts", outputDir + "/paired-stats", minusWindow)

      // 8. generate paired stats of different kinds
      generateDictSequenceStats(spark, outputDir + "/dicts", outputDir + "/dict-sequence-stats", minusWindow)

      // 9. generate paired stats of different kinds
      generateStatsSequenceStats(spark, outputDir + "/stats", outputDir + "/stats-sequence-stats", minusWindow)

      // 10. generate trends
      generateTrends(spark, outputDir + "/dicts", outputDir + "/trends")

      // 11. analyze trends
      generateTrendsAnalysis(spark, outputDir + "/trends", outputDir + "/trends-analysis")

      // 12. generate trends dataset
      generateTrendsDataset(spark, outputDir + "/trends-analysis", outputDir + "/trends-dataset")

      // stop
      spark.stop()
    }

    def readBaseData(spark: SparkSession, inputFile: String, outputPath: String, props: Properties) = {
      import spark.implicits._
      import spark.sqlContext.implicits._

      // read the tsv file
      val xtsv = TSV.read(inputFile, "\t")

      // read the data
      val cols = Seq("id_level1", "id_level2", "uuid", "node_id", "node_label", "parent_id", "event_name", "event_id", "ts", "anchor_ts", "event_set")
      println(cols)

      val tuples = xtsv.to_tuples11(cols, "readBaseData")
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
      if (true && Utils.checkIfExists(spark, outputPath) == true) {
        println("doEnrichment: outputPath already exists. Skipping ... : " + outputPath)
        return
      }

      val base = spark.read.parquet(inputPath)
      base.createOrReplaceTempView("base")

      // base kv
      val baseKV = spark.sql("select id_level1, id_level2, uuid, node_id, node_label, parent_id, event_name, event_id, ts, anchor_ts, event_set from base").rdd.map({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val node_id = row.getString(row.fieldIndex("node_id"))
        val node_label = row.getString(row.fieldIndex("node_label"))
        val parent_id = row.getString(row.fieldIndex("parent_id"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val event_id = row.getString(row.fieldIndex("event_id"))
        val ts = row.getString(row.fieldIndex("ts"))
        val anchor_ts = row.getString(row.fieldIndex("anchor_ts"))
        val event_set = row.getString(row.fieldIndex("event_set"))

        ((id_level1, id_level2, event_id, ts), (uuid, node_id, node_label, parent_id, event_name, anchor_ts, event_set))
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
        val local_datetime = Instant.ofEpochMilli(localTimeEpochMS).atZone(ZoneId.systemDefault()).toLocalDateTime()
        val local_datetime_num = "%04d%02d%02d%02d".format(local_datetime.getYear, local_datetime.getMonthValue, local_datetime.getDayOfMonth, local_datetime.getHour).toLong
        val local_hour = local_datetime.getHour
        val local_day_of_week = local_datetime.getDayOfWeek.getValue()

        ((id_level1, id_level2, event_id, ts), (local_datetime_num, local_hour, local_day_of_week))
      })

      // join
      val joined = (baseKV join dayOfWeekKV).map({
        case ((id_level1, id_level2, event_id, ts), ((uuid, node_id, node_label, parent_id, event_name, anchor_ts, event_set), (local_datetime_num, local_hour, local_day_of_week))) =>
          val keyValues = List(
            ("local_date_time_num", local_datetime_num.toString),
            ("local_hour", local_hour.toString),
            ("local_day_of_week", local_day_of_week.toString)
          )
          .toMap
          (id_level1, id_level2, uuid, node_id, node_label, parent_id, event_name, event_id, ts, anchor_ts, event_set, keyValues)
      })

      // create dataframe with keyvalues
      val cols = Seq("id_level1", "id_level2", "uuid", "node_id", "node_label", "parent_id", "event_name", "event_id", "ts", "anchor_ts", "event_set", "kv")
      val df = spark.createDataFrame(joined).toDF(cols:_*)

      // persist
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // clear
      base.unpersist()
      spark.catalog.dropTempView("base")
    }

    /**
     * Method to generate the node hierarchy
     */
    def createHierarchy(spark: SparkSession, inputPath: String, numLevels: Int, outputPath: String): Unit = {
      import spark.implicits._
      import spark.sqlContext.implicits._

      // check if already exists
      if (true && Utils.checkIfExists(spark, outputPath) == true) {
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
        val uuid = row.getString(row.fieldIndex("uuid"))
        val node_id = row.getString(row.fieldIndex("node_id"))
        val parent_id = row.getString(row.fieldIndex("parent_id"))
        val node_label = row.getString(row.fieldIndex("node_label"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val event_id = row.getString(row.fieldIndex("event_id"))
        val ts = row.getString(row.fieldIndex("ts"))
        val anchor_ts = row.getString(row.fieldIndex("anchor_ts"))
        val event_set = row.getString(row.fieldIndex("event_set"))
        val kv = row.getMap(row.fieldIndex("kv")).asInstanceOf[Map[String, String]]

        ((id_level1, id_level2, event_id, ts), (uuid, node_id, parent_id, node_label, event_name, anchor_ts, event_set, kv))
      })

      // take final ancestor level
      val ancestorLevelKV = ancestorLevel
        .map({ case (id_level1, id_level2, event_id, level_ppid, ts, ppid_list, ppid_label_list) =>
          ((id_level1, id_level2, event_id, ts), (ppid_list, ppid_label_list))
        })

      // join
      val joined = (baseKV leftOuterJoin ancestorLevelKV)
        .map({ case ((id_level1, id_level2, event_id, ts), ((uuid, node_id, parent_id, node_label, event_name, anchor_ts, event_set, kv), ancestorOpt)) =>
          val (ppid_list, ppid_label_list) = ancestorOpt.getOrElse((List.empty, List.empty))

          // append the new columns to kv
          val ancestry_kv = List(
            ("ancestor_ids", ppid_list.filter(_ != "").mkString(",")),
            ("ancestor_labels", ppid_label_list.filter(_ != "").mkString(",")),
            ("ancestor_count", ppid_list.length.toString)
          ).toMap

          (id_level1, id_level2, uuid, node_id, parent_id, node_label, event_name, event_id, ts, anchor_ts, event_set, kv, ancestry_kv)
        })

      // create dataframe
      val cols = List("id_level1", "id_level2", "uuid", "node_id", "parent_id", "node_label", "event_name", "event_id", "ts", "anchor_ts", "event_set", "kv", "ancestry_kv")
      val df = spark.createDataFrame(joined).toDF(cols:_*)

      // persist
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // clear
      base.unpersist()
      spark.catalog.dropTempView("base")
    }

    /**
     * Method to add one level parent informaton. TODO: The timestamp handling can have side effect, and need proper config parameters
     */
    def addParent(nodeLevel: RDD[(String, String, String, String, String, List[String], List[String])], parentInfo: RDD[(String, String, String, String, String, String)],
      numReducers: Int = 10) = {
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

    def createGroups(spark: SparkSession, inputPath: String, eventMappingFunc: (String) => String, kvMappingFunc: Map[String, String] => Map[String, String],
      outputPath: String): Unit = {
      import spark.implicits._
      import spark.sqlContext.implicits._

      // check if already exists
      if (true && Utils.checkIfExists(spark, outputPath) == true) {
        println("createGroups: outputPath already exists. Skipping ... : " + outputPath)
        return
      }

      // read base
      val base = spark.read.parquet(inputPath)
      base.createOrReplaceTempView("base")

      // create rdd
      val rdd = spark.sql("select * from base").rdd.map({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val node_id = row.getString(row.fieldIndex("node_id"))
        val parent_id = row.getString(row.fieldIndex("parent_id"))
        val node_label = row.getString(row.fieldIndex("node_label"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val event_id = row.getString(row.fieldIndex("event_id"))
        val ts = row.getString(row.fieldIndex("ts"))
        val anchor_ts = row.getString(row.fieldIndex("anchor_ts"))
        val event_set = row.getString(row.fieldIndex("event_set"))
        val kv = row.getMap(row.fieldIndex("kv")).asInstanceOf[Map[String, String]]
        val ancestry_kv = row.getMap(row.fieldIndex("ancestry_kv")).asInstanceOf[Map[String, String]]

        // group
        val event_name2 = eventMappingFunc(event_name)
        val kv2 = kvMappingFunc(kv)

        (id_level1, id_level2, uuid, node_id, parent_id, node_label, event_name2, event_id, ts, anchor_ts, event_set, kv2, ancestry_kv, event_name, kv)
      })

      // create dataframe with keyvalues
      val cols = Seq("id_level1", "id_level2", "uuid", "node_id", "parent_id", "node_label", "event_name", "event_id", "ts", "anchor_ts", "event_set", "kv", "ancestry_kv", "event_name_org", "kv_org")
      val df = spark.createDataFrame(rdd).toDF(cols:_*)

      // persist
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // clear
      base.unpersist()
      spark.catalog.dropTempView("base")
    }

    def createDicts(spark: SparkSession, inputPath: String, outputPath: String, numReducers: Int = 10): Unit = {
      createDict(spark, inputPath, outputPath, "ANY", EmptyVal, Empty1, EmptyVal, Empty2, EmptyVal, Empty3, numReducers = numReducers)
      createDict(spark, inputPath, outputPath, "event_name1", "kv.local_hour", "local_hour", EmptyVal, Empty2, EmptyVal, Empty3, numReducers = numReducers)
    }

    def createDict(spark: SparkSession, inputPath: String, outputPath: String, event_name: String, col_name1: String, col_alias1: String,
      col_name2: String, col_alias2: String, col_name3: String, col_alias3: String, numReducers: Int = 10): Unit = {
      import spark.implicits._
      import spark.sqlContext.implicits._

      // create output file
      val dictOutputPath = outputPath + "/devent_name=" + event_name + "/dcol_name1=" + col_alias1 + "/dcol_name2=" + col_alias2 + "/dcol_name3=" + col_alias3
      if (true && Utils.checkIfExists(spark, dictOutputPath)) {
        println("createDict: output already exists. Skipping... : " + dictOutputPath)
        return
      }

      // TODO
      println("createDict: [WARN]: there is a weird -1 in computing start_ts_diff_sec")

      // read base data
      val base = spark.read.parquet(inputPath)
      base.createOrReplaceTempView("base")

      // special filter for wild card in event name
      val eventNameFilter = (if (event_name == "ANY") "event_name != ''" else "event_name = '%s'".format(event_name))

      // construct query
      val query = ("select id_level1, id_level2, uuid, ts, anchor_ts, event_set, %s as %s, %s as %s, %s as %s from base where %s").format(
        col_name1, col_alias1, col_name2, col_alias2, col_name3, col_alias3, eventNameFilter)
      println("Query: %s".format(query))

      val rdd = spark.sql(query).rdd.flatMap({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val ts = row.getString(row.fieldIndex("ts"))
        val anchor_ts = row.getString(row.fieldIndex("anchor_ts"))
        val event_set = row.getString(row.fieldIndex("event_set"))

        val col_value1 = Utils.convertNullToEmptyString(if (col_name1 == EmptyVal) "" else row.getString(6))
        val col_value2 = Utils.convertNullToEmptyString(if (col_name2 == EmptyVal) "" else row.getString(7))
        val col_value3 = Utils.convertNullToEmptyString(if (col_name3 == EmptyVal) "" else row.getString(8))

        // TODO: FIXME
        val anchor_ts_sec = (if (anchor_ts != "") anchor_ts.toLong else 0L)
        val anchor_ts_str = anchor_ts

        // val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE
        // val maxLocalDate = LocalDate.parse(maxDateBC.value, dateTimeFormatter)
        // val maxLocalDayOfYear = maxLocalDate.getDayOfYear()
        // val localDate = LocalDate.parse(replaceMeDt, dateTimeFormatter)
        // val localDayOfYear = localDate.getDayOfYear()
        // val weekNumber = ((maxLocalDayOfYear - localDayOfYear) / 7).toInt + 1
        // val localEndOfWeekDate = maxLocalDate.minusDays((weekNumber - 1) * 7)
        // val localEndOfWeekDateStr = localEndOfWeekDate.format(dateTimeFormatter)
        // val monthNumber = ((maxLocalDayOfYear - localDayOfYear) / 28).toInt + 1
        // val localEndOfMonthDate = maxLocalDate.minusDays((monthNumber - 1) * 28)
        // val localEndOfMonthDateStr = localEndOfMonthDate.format(dateTimeFormatter)
        // val localYear = localDate.getYear() + "-00-00"

        // datetime for utcTimestamp
        val utc_ts_sec = (if (ts != "") ts.toLong else 0L)
        val utc_datetime = new DateTime(utc_ts_sec * 1000, DateTimeZone.UTC)
        val anchor_datetime = new DateTime(anchor_ts_sec * 1000, DateTimeZone.UTC)
        val diff_from_anchor = utc_ts_sec - anchor_ts_sec

        // windows for fixed time slots
        val hourly3Win = (utc_datetime.hourOfDay().getAsString().toInt / 3).toInt * 3
        val hourly6Win = (utc_datetime.hourOfDay().getAsString().toInt / 6).toInt * 6
        val hourly8Win = (utc_datetime.hourOfDay().getAsString().toInt / 8).toInt * 8
        val hourly12Win = (utc_datetime.hourOfDay().getAsString().toInt / 12).toInt * 12
        val minute15Win = (utc_datetime.minuteOfHour().getAsString().toInt / 15).toInt * 15
        val minute10Win = (utc_datetime.minuteOfHour().getAsString().toInt / 10).toInt * 10
        val minute5Win = (utc_datetime.minuteOfHour().getAsString().toInt / 5).toInt * 5
        val minute2Win = (utc_datetime.minuteOfHour().getAsString().toInt / 2).toInt * 2
        val minute1Win = (utc_datetime.minuteOfHour().getAsString().toInt / 1).toInt * 1

        // windows for time slots relative to anchor timestamp
        val xday01Win = ("xday01", 24 * 60 * 60)
        val xday07Win = ("xday07", 7 * 24 * 60 * 60)
        val xminute60Win = ("xminute60", 60 * 60)
        val xminute10Win = ("xminute10", 10 * 60)
        val xminute05Win = ("xminute05", 5 * 60)
        val xminute02Win = ("xminute02", 2 * 60)
        val xminute01Win = ("xminute01", 1 * 60)

        Some((utc_ts_sec, hourly3Win, diff_from_anchor))
        // fixed time agg values. TODO: this replaceMeDt and eventHour will fail when boundaries change
        val replaceMeDt = "20230101"
        val eventHour = "0"
        val agg_values = List(
          // ("minute01", replaceMeDt + "-" + eventHour + "%02d00".format(minute1Win), 0, "offset_zero"),
          // ("minute02", replaceMeDt + "-" + eventHour + "%02d00".format(minute2Win), 0),
          // ("minute05", replaceMeDt + "-" + eventHour + "%02d00".format(minute5Win), 0, "offset_zero"),
          // ("minute10", replaceMeDt + "-" + eventHour + "%02d00".format(minute10Win), 0),
          // ("minute15", replaceMeDt + "-" + eventHour + "%02d00".format(minute15Win)),
          ("hourly", replaceMeDt + "-" + "%02d0000".format(eventHour.toInt), 0)
          // ("hourly3", replaceMeDt + "-" + "%02d0000".format(hourly3Win)),
          // ("hourly6", replaceMeDt + "-" + "%02d0000".format(hourly6Win)),
          // ("hourly8", replaceMeDt + "-" + "%02d0000".format(hourly8Win)),
          // ("hourly12", replaceMeDt + "-"+ "%02d0000".format(hourly12Win)),
          // ("daily", date)
          // ("weekly", localEndOfWeekDateStr),
          // ("monthly", localEndOfMonthDateStr),
          // ("yearly", localYear)
        )

        // offset the time at which windows are pivoted
        val anchor_offsets = List(
          (0,  "offset_sec00")
          // (5,  "offset_sec05"),
          // (30, "offset_sec30"),
          // (60, "offset_sec60")
        )

        // relative time agg values. TODO :This -1 is  weird
        val xagg_values = for {
          // (xagg_value_name, xagg_win_size_sec) <- List(xminute01Win, xminute02Win, xminute05Win)
          (xagg_value_name, xagg_win_size_sec) <- List(xminute60Win, xday01Win, xday07Win)
          (anchor_offset, anchor_offset_label) <- anchor_offsets
        } yield {
          val diff_from_anchor_with_offset = diff_from_anchor - anchor_offset
          val start_ts_diff_sec = {
            if (diff_from_anchor_with_offset >= 0)
              (diff_from_anchor_with_offset / xagg_win_size_sec).toInt * xagg_win_size_sec
            else
              (diff_from_anchor_with_offset / xagg_win_size_sec - 1).toInt * xagg_win_size_sec
          }
          val xagg_value = anchor_datetime.plusSeconds(start_ts_diff_sec)
          val xagg_value_start_sec = (xagg_value.getMillis / 1000).toLong
          val xagg_value_end_sec = xagg_value_start_sec + xagg_win_size_sec
          val is_anchor_window = (if ((anchor_ts_sec >= xagg_value_start_sec) && (anchor_ts_sec < xagg_value_end_sec)) 1 else 0)
          val is_post_anchor_v2 = (if (xagg_value_start_sec >= anchor_ts_sec) 1 else 0)
          val date_window = "20%02d-%02d-%02d-%02d%02d%02d".format(
            xagg_value.yearOfCentury().getAsString().toInt,
            xagg_value.monthOfYear().getAsString().toInt,
            xagg_value.dayOfMonth().getAsString().toInt,
            xagg_value.hourOfDay().getAsString().toInt,
            xagg_value.minuteOfHour().getAsString().toInt,
            xagg_value.secondOfMinute().getAsString().toInt
          )
          (xagg_value_name, date_window, is_anchor_window, anchor_offset_label, is_post_anchor_v2)
        }

        println("xagg_values: " + xagg_values)

        // generate multiple values depending upon how far away the event was after anchorion
        val anchor_time_win_values = {
          val buffer = scala.collection.mutable.ListBuffer[String]()
          buffer.append("inf")
          // if (diff_from_anchor <= 60 * 1) buffer.append("min01")
          // if (diff_from_anchor <= 60 * 2) buffer.append("min02")
          // if (diff_from_anchor <= 60 * 5) buffer.append("min05")
          // if (diff_from_anchor <= 60 * 10) buffer.append("min10")
          // if (diff_from_anchor <= 60 * 60) buffer.append("-hour")
          buffer.toList
        }

        // apply the filter to the grouping key. TODO
        // val hasAnchorSplGroup = anchorParentLevel match {
        //   case 2 => "anchor_parent_id"
        //   case 1 => "anchor_node_id"
        //   case 0 => "anchor"
        // }

        // run generator to create all possible values
        val generatedValues = {
          (for {
            anchor_time_win_value <- anchor_time_win_values
            (agg_key_name, agg_key_value, is_anchor_window, anchor_offset_label, is_post_anchor_v2) <- (xagg_values)
          } yield {
            Seq(
             ((id_level1, id_level2, "", "%s-%s-%s-%s".format("level2", anchor_offset_label, anchor_time_win_value, event_set), event_name, col_alias1, col_alias2, col_alias3,
               agg_key_name, agg_key_value, anchor_ts_str, event_set, col_value1, col_value2, col_value3, is_anchor_window, is_post_anchor_v2), 1),
             ((id_level1, "", "", "%s-%s-%s-%s".format("level1", anchor_offset_label, anchor_time_win_value, event_set), event_name, col_alias1, col_alias2, col_alias3,
               agg_key_name, agg_key_value, anchor_ts_str, event_set, col_value1, col_value2, col_value3, is_anchor_window, is_post_anchor_v2), 1)
            )
          })
          .flatten
        }

        // pass to the reduce function
        generatedValues
      })
      .reduceByKey({ case (c1, c2) => c1 + c2 })
      .map({ case ((id_level1, id_level2, uuid, grouping_level, event_name_file, col_alias1, col_alias2, col_alias3,
        agg_key_name, agg_key_value, anchor_ts_str, event_set, col_value1, col_value2, col_value3, is_anchor_window, post_anchor_flag), count) =>
        ((id_level1, id_level2, uuid, grouping_level, event_name_file, col_alias1, col_alias2, col_alias3,
          agg_key_name, agg_key_value, anchor_ts_str, event_set, is_anchor_window, post_anchor_flag), (col_value1, col_value2, col_value3, count))
      })
      .groupByKey(numReducers)
      .flatMap({ case ((id_level1, id_level2, uuid, grouping_level, event_name_file, col_alias1, col_alias2, col_alias3,
        agg_key_name, agg_key_value, anchor_ts_str, event_set, is_anchor_window, post_anchor_flag), vs) =>

        // get list
        val vs2 = vs.toList

        // buffer for features
        val buffer = new scala.collection.mutable.ListBuffer[(String, String, String, String, Int)]

        // generate unary features
        val unaryFeatures = for {
          (col_value1_1, col_value2_1, col_value3_1, count_1) <- vs2
        } yield {
           buffer.append((col_value1_1, col_value2_1, col_value3_1, "unary", count_1))
        }

        // run binary features only for LevelNodeId
        // generate binary features
        if (true) {
          val binary_features = for {
            ((col_value1_1, col_value2_1, col_value3_1, count_1), index_1) <- vs2.zipWithIndex
            ((col_value1_2, col_value2_2, col_value3_2, count_2), index_2) <- vs2.zipWithIndex
            if (col_value1_1 < col_value1_2 && col_value2_1 == col_value2_2 && col_value3_1 == col_value3_2)
          } yield {
            buffer.append((col_value1_1 + "_" + col_value1_2, col_value2_1, col_value3_1, "binary", Seq(count_1, count_2).min))
          }

          val vs2Selected = vs2.filter({ case (col_value1, col_value2, col_value3, count) =>
            SelectedBinaryFeatures.contains(col_value1) == true && ExcludedEventGroupNames.contains(col_value1) == false
          })

          // val vs2Selected = vs2
          val ternary_features = for {
            ((col_value1_1, col_value2_1, col_value3_1, count_1), index_1) <- vs2Selected.zipWithIndex
            ((col_value1_2, col_value2_2, col_value3_2, count_2), index_2) <- vs2Selected.zipWithIndex
            ((col_value1_3, col_value2_3, col_value3_3, count_3), index_3) <- vs2Selected.zipWithIndex
            if (col_value1_1 < col_value1_2 && col_value2_1 == col_value2_2 && col_value3_1 == col_value3_2)
            if (col_value1_2 < col_value1_3 && col_value2_2 == col_value2_3 && col_value3_2 == col_value3_3)
          } yield {
            buffer.append((col_value1_1 + "_" + col_value1_2 + "_" + col_value1_3, col_value2_1, col_value3_1, "ternary", Seq(count_1, count_2, count_3).min))
          }
        }

        // run a local group by
        // combine the features to generate output data
        val grouped = buffer
          .groupBy({ case (col_value1, col_value2, col_value3, cardinality, count) => (col_value1, col_value2, col_value3, cardinality) })
          .map({ case ((col_value1, col_value2, col_value3, cardinality), bvs) => (col_value1, col_value2, col_value3, cardinality, bvs.map({ case (_, _, _, _, count) => count }).sum ) })
          .flatMap({ case (col_value1, col_value2, col_value3, cardinality, count) =>
            // check for min count
            if (count >= 0) {
              Some((id_level1.toString, id_level2, uuid, grouping_level, event_name_file, col_alias1, col_alias2, col_alias3,
                agg_key_name, agg_key_value, anchor_ts_str, event_set, is_anchor_window.toString, post_anchor_flag.toString, col_value1, col_value2, col_value3, cardinality, count))
            } else {
              None
            }
          })

        grouped
      })

      // create dataframe
      val cols = Seq("id_level1", "id_level2", "uuid", "grouping_level", "event_name",
        "col_name1", "col_name2", "col_name3", "agg_key_name", "agg_key_value", "anchor_ts_str", "event_set", "is_anchor_window", "post_anchor_flag",
        "col_value1", "col_value2", "col_value3", "cardinality", "count")
      val df = spark.createDataFrame(rdd).toDF(cols: _*)

      // persist
      df.write.mode(SaveMode.Overwrite).parquet(dictOutputPath)

      // clear
      base.unpersist()
      spark.catalog.dropTempView("base")
   }

   def generateStats(spark: SparkSession, dictPath: String, outputPath: String, numReducers: Int = 10): Unit = {
      import spark.implicits._
      import spark.sqlContext.implicits._

      // check if output already exists
      if (!true && Utils.checkIfExists(spark, outputPath)) {
        println("generateStats: output already exists. Skipping... : " + outputPath)
        return
      }

      // read input
      val dict = spark.read.parquet(dictPath)
      dict.createOrReplaceTempView("dict")

      // query
      val query = "select id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag," +
        " event_name, col_name1, col_name2, col_name3, col_value1, col_value2, col_value3, count from dict"

      // debug
      println("generateStats: query: " + query)

      // compute stats
      val stats = spark.sql(query).rdd.map({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val grouping_level = row.getString(row.fieldIndex("grouping_level"))
        val agg_key_name = row.getString(row.fieldIndex("agg_key_name"))
        val agg_key_value = row.getString(row.fieldIndex("agg_key_value"))
        val event_set = row.getString(row.fieldIndex("event_set"))
        val post_anchor_flag = row.getString(row.fieldIndex("post_anchor_flag"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val col_name1 = row.getString(row.fieldIndex("col_name1"))
        val col_name2 = row.getString(row.fieldIndex("col_name2"))
        val col_name3 = row.getString(row.fieldIndex("col_name3"))
        val col_value1 = row.getString(row.fieldIndex("col_value1"))
        val col_value2 = row.getString(row.fieldIndex("col_value2"))
        val col_value3 = row.getString(row.fieldIndex("col_value3"))
        val count = row.getInt(row.fieldIndex("count"))

        // tuples
        ((id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3),
          (col_value1, col_value2, col_value3, count))
      })
      .groupByKey(numReducers)
      .map({ case ((id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3), vs) =>
        // take care of iterators
        val vs2 = vs.toList

        // total count
        val total_count = vs2.map({ case (col_value1, col_value2, col_value3, count) => count }).sum

        // unique entries count
        val uniq_count = vs2.size

        // entropy
        val entropy = vs2.map({ case (col_value1, col_value2, col_value3, count) =>
          val prob = math.max(math.min((if (total_count > 0) 1f * count / total_count else 0f), 1f), 0f)
          if (prob != 0) -1f * prob * math.log(prob) / math.log(2) else 0f
        }).sum

        (id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3,
          uniq_count, total_count, "%.6f".format(entropy).toFloat)
      })

      // create dataframe
      val df = spark.createDataFrame(stats)
        .toDF("id_level1", "id_level2", "uuid", "grouping_level", "agg_key_name", "agg_key_value", "event_set", "post_anchor_flag", "event_name",
          "col_name1", "col_name2", "col_name3", "uniq_count", "total_count", "entropy")

      // persist
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // unpersist
      dict.unpersist()
      spark.catalog.dropTempView("dict")
    }


    def computeHash(nonce: String, value: String) = {
      math.abs((nonce + value).hashCode())
    }

    def computeCosSim(x: Map[String, Int], y: Map[String, Int]) = {
      if (x.size == 0 || y.size == 0 || x.size != y.size) {
        0f
      } else {
        val combined = (x.keys.toSet union y.keys.toSet).toList.sorted
        val xvect = combined.map({ c => x.get(c).getOrElse(0) })
        val yvect = combined.map({ c => y.get(c).getOrElse(0) })

        val dp = xvect.zip(yvect).map({ case (x1, y1) => x1 * y1 }).sum
        val xLen = math.sqrt(xvect.map({ x1 => x1 * x1 }).sum)
        val yLen = math.sqrt(yvect.map({ y1 => y1 * y1 }).sum)

        "%.6f".format((1.0 * dp / (xLen * yLen).toFloat)).toFloat
      }
    }

    def computeStats(y: List[Float]) = {
      val x = y.sorted
      if (x.size == 0) {
        (0f, 0f, 0f, 0f)
      } else {
        val mean = 1f * x.sum / x.size
        val median = x((x.size * 0.5).toInt)
        val stdDev = math.sqrt(x.map({ x1 => (x1 - mean) * (x1 - mean) }).sum / x.size.toFloat)
        val mad = x((x.size * 0.75).toInt) -  x((x.size * 0.25).toInt)

        (mean.toFloat, median, stdDev.toFloat, mad.toFloat)
      }
    }

    def computePercStats(y: List[Float]) = {
      val x = y.sorted
      if (x.size == 0) {
        (0f, 0f, 0f, 0f)
      } else {
        val perc05 = x((x.size * 0.05).toInt)
        val perc10 = x((x.size * 0.10).toInt)
        val perc90 = x((x.size * 0.90).toInt)
        val perc95 = x((x.size * 0.95).toInt)

        val madAlpha5 = (perc95 - perc05)
        val madAlpha10 = (perc90 - perc10)

        (perc90, perc95, madAlpha10, madAlpha5)
      }
    }

    def getPercentile(x: Float, y: List[Float]) = {
      1f * y.filter({ y1 => x >= y1 }).size / y.size
    }

    def computeJacSim(x: Set[String], y: Set[String]) = {
      1f * (x intersect y).size / (x union y).size
    }

    def generatePairedStats(spark: SparkSession, dictPath: String, outputPath: String, minusWindow: Int, numReducers: Int = 10): Unit = {
      import spark.implicits._
      import spark.sqlContext.implicits._

      // check if output exists already
      if (false && Utils.checkIfExists(spark, outputPath)) {
        println("generatePairedStats: output already exists. Skipping... : " + outputPath)
        return
      }

      // read data
      val dict = spark.read.parquet(dictPath)
      dict.createOrReplaceTempView("dict")

      // query
      val query = "select id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag," +
        " event_name, col_name1, col_name2, col_name3, col_value1, col_value2, col_value3, count from dict"

      // debug
      println("generatePairedStats: query: " + query)

      val emptySet: Set[String] = Set.empty
      val emptyMap: Map[String, Int] = Map.empty
      val pairedStats = spark.sql(query).coalesce(1000).rdd.map({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val grouping_level = row.getString(row.fieldIndex("grouping_level"))
        val agg_key_name = row.getString(row.fieldIndex("agg_key_name"))
        val agg_key_value = row.getString(row.fieldIndex("agg_key_value"))
        val event_set = row.getString(row.fieldIndex("event_set"))
        val post_anchor_flag = row.getString(row.fieldIndex("post_anchor_flag"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val col_name1 = row.getString(row.fieldIndex("col_name1"))
        val col_name2 = row.getString(row.fieldIndex("col_name2"))
        val col_name3 = row.getString(row.fieldIndex("col_name3"))
        val col_value1 = row.getString(row.fieldIndex("col_value1"))
        val col_value2 = row.getString(row.fieldIndex("col_value2"))
        val col_value3 = row.getString(row.fieldIndex("col_value3"))
        val count = row.getInt(row.fieldIndex("count"))

        // tuples
        ((id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3),
          (col_value1, col_value2, col_value3, count))
      })
      .groupByKey(numReducers)
      .map({ case ((id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3), vs) =>
        ((id_level1, id_level2, uuid, grouping_level, agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3), (agg_key_value, vs))
      })
      .groupByKey(numReducers)
      .flatMap({  case ((id_level1, id_level2, uuid, grouping_level, agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3), vs) =>
        // read list
        val vs2 = vs.toList.sortBy({ case (agg_key_value, _) => agg_key_value })

        // iterate over all possible windows
        (0 to minusWindow).filter({ minusWindowValue => vs2.size - minusWindowValue > 0 }).map({ minusWindowValue =>
          val vs2MinusWindow = vs2.take(vs2.size - minusWindowValue)
          val vs2PlusWindow = vs2.drop(vs2.size - minusWindowValue)

          // compute jacSim
          val jacSimValues = vs2MinusWindow.map({ case (agg_key_value, cs) =>
            cs.map({ case (col_value1, col_value2, col_value3, count) => List(col_value1, col_value2, col_value3).mkString(":") }).toSet
          })

          val jacSimValues2 = emptySet :: jacSimValues
          val jacSimPairs = jacSimValues2.take(jacSimValues2.size - 1).zip(jacSimValues2.drop(1))
          val jsValues = jacSimPairs.map({ case (col_values1, col_values2) => computeJacSim(col_values1, col_values2) })
          // FIXME: tail
          val (jsMean, jsMedian, jsStdDev, jsMad) = computeStats(jsValues.tail)

          // inferencing list
          val jsInferences = vs2PlusWindow.map({ case (agg_key_value, cs) =>
            val valuesSet = cs.map({ case (col_value1, col_value2, col_value3, count) => List(col_value1, col_value2, col_value3).mkString(":") }).toSet
            computeJacSim(valuesSet, jacSimValues2.last)
          })

          val jsInferencesMean = (if (jsInferences.size > 0) 1f * jsInferences.sum / jsInferences.size else -1f)

          val cosSimMapValues = vs2MinusWindow.map({ case (agg_key_value, cs) =>
            cs.map({ case (col_value1, col_value2, col_value3, count) => (List(col_value1, col_value2, col_value3).mkString(":"), count) }).toMap
          })

          // println("generatePairedStats: jacSimValues2: " + jacSimValues2)
          val cosSimMapValues2 = emptyMap :: cosSimMapValues
          // println("generatePairedStats: cosSimMapValues2: " + cosSimMapValues2)
          val cosSimPairs = cosSimMapValues2.take(cosSimMapValues2.size - 1).zip(cosSimMapValues2.drop(1))
          val csSimValues = cosSimPairs.map({ case (colValuesMap1, colValuesMap2) => computeCosSim(colValuesMap1, colValuesMap2) })

          // FIXME: tail
          val (cosSimMean, cosSimMedian, cosSimStdDev, cosSimMad) = computeStats(csSimValues.tail)

          // cos sim inferences
          val cosSimInferences = vs2PlusWindow.map({ case (agg_key_value, cs) =>
            val valuesMap = cs.map({ case (col_value1, col_value2, col_value3, count) => (List(col_value1, col_value2, col_value3).mkString(":"), count) }).toMap
            computeCosSim(valuesMap, cosSimMapValues2.last)
          })
          val cosSimInferencesMean = (if (cosSimInferences.size > 0) 1f * cosSimInferences.sum / cosSimInferences.size else -1f)

          // new agg key name
          val new_agg_key_name = (if (minusWindowValue == 0) agg_key_name else agg_key_name + "_minus_" + minusWindowValue)

          // generate output
          (id_level1, id_level2, uuid, grouping_level, new_agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3,
            vs2.size, jsMean, jsStdDev, jsInferencesMean, jsInferences.map(x => "%.6f".format(x)).mkString(","),
            cosSimMean, cosSimStdDev, cosSimInferencesMean, cosSimInferences.map(x => "%.6f".format(x)).mkString(","))
        })
      })

      // create dataframe
      val df = spark.createDataFrame(pairedStats)
        .toDF("id_level1", "id_level2", "uuid", "grouping_level", "agg_key_name", "event_set", "post_anchor_flag", "event_name", "col_name1", "col_name2", "col_name3",
          "metricsSize", "jsMean", "jsStdDev", "jsInferencesMean", "jsInferences",
          "cosSimMean", "cosSimStdDev", "cosSimInferencesMean", "cosSimInferences")

      // persist
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // unpersist
      dict.unpersist()
      spark.catalog.dropTempView("dict")
    }

    def generateDictSequenceStats(spark: SparkSession, dictPath: String, outputPath: String, minusWindow: Int, numReducers: Int = 10): Unit = {
      // check if exists already
      if (Utils.checkIfExists(spark, outputPath)) {
        println("generateDictSequenceStats: output already exists. Skipping... : " + outputPath)
        return
      }

      // read data
      val dict = spark.read.parquet(dictPath)
      dict.createOrReplaceTempView("dict")

      // aggregate
      val query = "select id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag," +
        " event_name, col_name1, col_name2, col_name3, col_value1, col_value2, col_value3, count from dict"

      // query
      println("generateDictSequenceStats: query: " + query)

      val emptySet: Set[String] = Set.empty
      val emptyMap: Map[String, Int] = Map.empty
      val seqStats = spark.sql(query).rdd.map({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val grouping_level = row.getString(row.fieldIndex("grouping_level"))
        val agg_key_name = row.getString(row.fieldIndex("agg_key_name"))
        val agg_key_value = row.getString(row.fieldIndex("agg_key_value"))
        val event_set = row.getString(row.fieldIndex("event_set"))
        val post_anchor_flag = row.getString(row.fieldIndex("post_anchor_flag"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val col_name1 = row.getString(row.fieldIndex("col_name1"))
        val col_name2 = row.getString(row.fieldIndex("col_name2"))
        val col_name3 = row.getString(row.fieldIndex("col_name3"))
        val col_value1 = row.getString(row.fieldIndex("col_value1"))
        val col_value2 = row.getString(row.fieldIndex("col_value2"))
        val col_value3 = row.getString(row.fieldIndex("col_value3"))
        val count = row.getInt(row.fieldIndex("count"))

        // tuples
        ((id_level1, id_level2, uuid, grouping_level, agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3,
          col_value1, col_value2, col_value3), (agg_key_value, count))
      })
      .groupByKey(numReducers)
      .flatMap({ case ((id_level1, id_level2, uuid, grouping_level, agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3,
        col_value1, col_value2, col_value3), vs) =>
        // get list
        val vs2 = vs.toList.sortBy({ case (agg_key_value, count) => agg_key_value })

        // iterate over all window sizes
        (0 to minusWindow).filter({ minusWindowValue => vs2.size - minusWindowValue > 0 }).map({ minusWindowValue =>
          val vs2MinusWindow = vs2.take(vs2.size - minusWindowValue)
          val vs2PlusWindow = vs2.drop(vs2.size - minusWindowValue)
          val counts = vs2MinusWindow.map({ case  (agg_key_value, count) => count })

          val num_count = vs2MinusWindow.size
          val minCount = counts.min
          val maxCount = counts.max
          val (meanCount, medianCount, stdDevCount, madCount) = computeStats(counts.map(_.toFloat))

          val meanOverStdDev = (if (stdDevCount != 0) meanCount / stdDevCount else -1f)
          val stdDevOverMean = (if (meanCount != 0) stdDevCount / meanCount else -1f)

          // inferences
          val vs2Inferences = vs2PlusWindow.map({ case (agg_key_value, count) =>
            val percIncreaseMax = {
              if (count > maxCount)
                if (maxCount > 0) 1f * count / maxCount else 1f * count
              else
                0f
            }
            percIncreaseMax
          })

          val vs2InferencesSorted = vs2Inferences.sorted

          val vs2InferencesPercOverMaxMean = (if (vs2Inferences.size > 0) 1f * vs2Inferences.sum / vs2Inferences.size else -1f)
          val vs2InferencesPercOverMaxMedian = (if (vs2InferencesSorted.size > 0) vs2InferencesSorted((vs2InferencesSorted.size * 0.5).toInt) else -1f)

          val count_kv_map = Map(
            ("meanCount", meanCount),
            ("medianCount", medianCount),
            ("stdDevCount", stdDevCount),
            ("madCount", madCount),
            ("meanOverStdDev", meanOverStdDev),
            ("stdDevOverMean", stdDevOverMean)
          )

          // base on the time window substraction, create a new one
          val new_agg_key_name = (if (minusWindowValue == 0) agg_key_name else agg_key_name + "_minus_" + minusWindowValue)

          // tuples
          (id_level1, id_level2, uuid, grouping_level, new_agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3,
            col_value1, col_value2, col_value3, num_count, minCount, maxCount, vs2InferencesPercOverMaxMean, vs2InferencesPercOverMaxMedian, count_kv_map)
        })
      })

      // create data frame
      val df = spark.createDataFrame(seqStats)
        .toDF("id_level1", "id_level2", "uuid", "grouping_level", "agg_key_name", "event_set", "post_anchor_flag", "event_name", "col_name1", "col_name2", "col_name3",
          "col_value1", "col_value2", "col_value3", "num_count", "minCount", "maxCount", "vs2InferencesPercOverMaxMean", "vs2InferencesPercOverMaxMedian", "count_kv_map")

      // persist
      df.coalesce(numReducers).write.mode(SaveMode.Overwrite).parquet(outputPath)

      // unpersist
      dict.unpersist()
      spark.catalog.dropTempView("dict")
    }

    def generateStatsSequenceStats(spark: SparkSession, statsPath: String, outputPath: String, minusWindow: Int, numReducers: Int = 10): Unit = {
      // check if output already exists
      if (false && Utils.checkIfExists(spark, outputPath)) {
        println("generateStatsSequenceStats: output already exists. Skipping... : " + outputPath)
        return
      }

      // read data
      val stats = spark.read.parquet(statsPath)
      stats.createOrReplaceTempView("stats")

      // aggregate
      val query = "select id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, event_set, post_anchor_flag," +
        " event_name, col_name1, col_name2, col_name3, uniq_count, total_count, entropy from stats"

      // debug
      println("generateStatsSequenceStats: query: " + query)

      val emptySet: Set[String] = Set.empty
      val emptyMap: Map[String, Int] = Map.empty
      val seqStats = spark.sql(query).rdd.map({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val grouping_level = row.getString(row.fieldIndex("grouping_level"))
        val agg_key_name = row.getString(row.fieldIndex("agg_key_name"))
        val agg_key_value = row.getString(row.fieldIndex("agg_key_value"))
        val event_set = row.getString(row.fieldIndex("event_set"))
        val post_anchor_flag = row.getString(row.fieldIndex("post_anchor_flag"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val col_name1 = row.getString(row.fieldIndex("col_name1"))
        val col_name2 = row.getString(row.fieldIndex("col_name2"))
        val col_name3 = row.getString(row.fieldIndex("col_name3"))
        val uniq_count = row.getInt(row.fieldIndex("uniq_count"))
        val total_count = row.getInt(row.fieldIndex("total_count"))
        val entropy = row.getFloat(row.fieldIndex("entropy"))

        // tuples
        ((id_level1, id_level2, uuid, grouping_level, agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3),
          (agg_key_value, uniq_count, total_count, entropy))
      })
      .distinct
      .groupByKey(numReducers)
      .flatMap({ case ((id_level1, id_level2, uuid, grouping_level, agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3), vs) =>
        // get list
        val vs2 = vs.toList.sortBy({ case (agg_key_value, uniq_count, total_count, entropy) => agg_key_value })

        (0 to minusWindow).filter({ minusWindowValue => vs2.size - minusWindowValue > 0 }).map({ minusWindowValue =>
          val vs2MinusWindow = vs2.take(vs2.size - minusWindowValue)
          val uniq_counts = vs2MinusWindow.map({ case  (agg_key_value, uniq_count, total_count, entropy) => uniq_count })
          val total_counts = vs2MinusWindow.map({ case  (agg_key_value, uniq_count, total_count, entropy) => total_count })
          val entropyCounts = vs2MinusWindow.map({ case  (agg_key_value, uniq_count, total_count, entropy) => entropy })

          // uniq_count
          val num_count = vs2MinusWindow.size
          val min_uniq_count = uniq_counts.min
          val max_uniq_count = uniq_counts.max
          val (meanUniqCount, medianUniqCount, stdDevUniqCount, madUniqCount) = computeStats(uniq_counts.map(_.toFloat))

          val meanOverStdDevUniq = (if (stdDevUniqCount != 0) meanUniqCount / stdDevUniqCount else -1f)
          val stdDevOverMeanUniq = (if (meanUniqCount != 0) stdDevUniqCount / meanUniqCount else -1f)

          // total_count
          val minTotalCount = total_counts.min
          val maxTotalCount = total_counts.max
          val (meanTotalCount, medianTotalCount, stdDevTotalCount, madTotalCount) = computeStats(total_counts.map(_.toFloat))

          val meanOverStdDevTotal = (if (stdDevTotalCount != 0) meanTotalCount / stdDevTotalCount else -1f)
          val stdDevOverMeanTotal = (if (meanTotalCount != 0) stdDevTotalCount / meanTotalCount else -1f)

          // entropy
          val minEntropyCount = entropyCounts.min
          val maxEntropyCount = entropyCounts.max
          val (meanEntropyCount, medianEntropyCount, stdDevEntropyCount, madEntropyCount) = computeStats(entropyCounts.map(_.toFloat))

          val meanOverStdDevEntropy = (if (stdDevEntropyCount != 0) meanEntropyCount / stdDevEntropyCount else -1f)
          val stdDevOverMeanEntropy = (if (meanEntropyCount != 0) stdDevEntropyCount / meanEntropyCount else -1f)

          val count_kv_map = Map(
            ("meanUniqCount", meanUniqCount),
            ("medianUniqCount", medianUniqCount),
            ("stdDevUniqCount", stdDevUniqCount),
            ("madUniqCount", madUniqCount),
            ("meanOverStdDevUniq", meanOverStdDevUniq),
            ("stdDevOverMeanUniq", stdDevOverMeanUniq),

            ("meanTotalCount", meanTotalCount),
            ("medianTotalCount", medianTotalCount),
            ("stdDevTotalCount", stdDevTotalCount),
            ("madTotalCount", madTotalCount),
            ("meanOverStdDevTotal", meanOverStdDevUniq),
            ("stdDevOverMeanTotal", stdDevOverMeanTotal),

            ("meanEntropyCount", meanEntropyCount),
            ("medianEntropyCount", medianEntropyCount),
            ("stdDevEntropyCount", stdDevEntropyCount),
            ("madEntropyCount", madEntropyCount),
            ("meanOverStdDevEntropy", meanOverStdDevEntropy),
            ("stdDevOverMeanEntropy", stdDevOverMeanEntropy)
          )

          // base on the time window substraction, create a new one
          val new_agg_key_name = (if (minusWindowValue == 0) agg_key_name else agg_key_name + "_minus_" + minusWindowValue)

          (id_level1, id_level2, uuid, grouping_level, new_agg_key_name, event_set, post_anchor_flag, event_name, col_name1, col_name2, col_name3,
            num_count, min_uniq_count, max_uniq_count, count_kv_map)
        })
      })

      // create data frame
      val df = spark.createDataFrame(seqStats)
        .toDF("id_level1", "id_level2", "uuid", "grouping_level", "agg_key_name", "event_set", "post_anchor_flag", "event_name", "col_name1", "col_name2", "col_name3",
          "num_count", "min_uniq_count", "max_uniq_count", "count_kv_map")

      // persist
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // unpersist
      stats.unpersist()
      spark.catalog.dropTempView("stats")
    }

    def generateTrends(spark: SparkSession, dictPath: String, outputPath: String, numReducers: Int = 10): Unit = {
      // check output path
      if (true && Utils.checkIfExists(spark, outputPath)) {
        println("generateTrends: output already exists. Skipping... : " + outputPath)
        return
      }

      println("generateTrends: [WARN]: selecting only 'unary' and not 'binary', 'ternary'")
      println("generateTrends: [WARN]: AggKeyNamesLearningWindowMap for hourly, xminute60 changed to 24 from 10")

      // constants to consider min values fror trends. Typically 10
      val AggKeyNamesLearningWindowMap = Map("daily" -> 7, "hourly" -> 24, "minute10" -> 12, "minute05" -> 24, "minute02" -> 10, "minute01" -> 10,
        "xday07" -> 8, "xday01" -> 7, "xminute60" -> 24, "xminute10" -> 12, "xminute05" -> 10, "xminute02" -> 10, "xminute01" -> 10)

      val AggKeyNamesMinutesMultiplierMap = Map("daily" -> 24*60, "hourly" -> 60, "minute10" -> 10, "minute05" -> 5, "minute02" -> 2, "minute01" -> 1,
        "xday07" -> 7*24*60, "xday01" -> 24*60, "xminute60" -> 60, "xminute10" -> 10, "xminute05" -> 5, "xminute02" -> 2, "xminute01" -> 1)

      // read dict
      val dict = spark.read.parquet(dictPath)
      dict.createOrReplaceTempView("dict")

      // aggregate trend values. TODO: there was a distinct here. FIXME: the special where conditions
      val query = "select id_level1, id_level2, uuid, grouping_level, agg_key_name, agg_key_value, anchor_ts_str, is_anchor_window, event_set, post_anchor_flag," +
        " event_name, col_name1, col_name2, col_name3, col_value1, col_value2, col_value3, cardinality, count from dict " +
        "where cardinality in ('unary') AND ((col_name3 = 'empty3') OR (col_name3 like 'AnchorSpl%NodeId' AND col_value3='1'))"

      // debug
      println("generateTrends: query: %s".format(query))

      // run query
      val aggTrendValues = spark.sql(query).rdd.map({ row =>
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val uuid = row.getString(row.fieldIndex("uuid"))
        val grouping_level = row.getString(row.fieldIndex("grouping_level"))
        val agg_key_name = row.getString(row.fieldIndex("agg_key_name"))
        val agg_key_value = row.getString(row.fieldIndex("agg_key_value"))
        val anchor_ts_str = row.getString(row.fieldIndex("anchor_ts_str"))
        val is_anchor_window = row.getString(row.fieldIndex("is_anchor_window"))
        val event_set = row.getString(row.fieldIndex("event_set"))
        val post_anchor_flag = row.getString(row.fieldIndex("post_anchor_flag"))
        val event_name = row.getString(row.fieldIndex("event_name"))
        val col_name1 = row.getString(row.fieldIndex("col_name1"))
        val col_name2 = row.getString(row.fieldIndex("col_name2"))
        val col_name3 = row.getString(row.fieldIndex("col_name3"))
        val col_value1 = row.getString(row.fieldIndex("col_value1"))
        val col_value2 = row.getString(row.fieldIndex("col_value2"))
        val col_value3 = row.getString(row.fieldIndex("col_value3"))
        val cardinality = row.getString(row.fieldIndex("cardinality"))
        val count = row.getInt(row.fieldIndex("count"))

        ((id_level1, id_level2, uuid, grouping_level, agg_key_name, anchor_ts_str, event_set, event_name, col_name1, col_name2, col_name3,
          col_value1, col_value2, col_value3, cardinality), (agg_key_value, (count, is_anchor_window, post_anchor_flag)))
      })
      .groupByKey(numReducers)
      .flatMap({ case ((id_level1, id_level2, uuid, grouping_level, agg_key_name, anchor_ts_str, event_set, event_name, col_name1, col_name2, col_name3,
          col_value1, col_value2, col_value3, cardinality), vs) =>

        // convert to list
        val vs2 = vs.toList

        // sort the list by agg_key_values. TODO: this inner loop is very expensive
        val vs2Map = vs2.toMap
        val agg_key_values_rev_sorted = vs2.map({ case (agg_key_value, _) => agg_key_value }).sorted.reverse

        // get learning window
        val learningWindow = AggKeyNamesLearningWindowMap(agg_key_name)

        // leave one for inferencing
        val trendValues = Range(0, agg_key_values_rev_sorted.size).flatMap({ case index =>
          // the value at index as what we are inferencing
          val inf_agg_key_value = agg_key_values_rev_sorted(index)

          // these learning values are the last N non zero values. we need to create the dense representation
          val infDateTime = LocalDateTime.parse(inf_agg_key_value, DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss"))
          val infDateTimeLastLearning = infDateTime.minusMinutes(learningWindow * AggKeyNamesMinutesMultiplierMap(agg_key_name))

          // orionDt in the same format as agg_key_value
          val infLastLearningValue = "%04d-%02d-%02d-%02d%02d00".format(infDateTimeLastLearning.getYear(), infDateTimeLastLearning.getMonthValue(), infDateTimeLastLearning.getDayOfMonth(),
            infDateTimeLastLearning.getHour(), infDateTimeLastLearning.getMinute())

          // generate dense learning values. TODO: Notice the reverse
          val fixedLengthLearningValues = agg_key_values_rev_sorted.drop(index + 1).take(learningWindow).reverse
          val fixedTimeLearningValues = fixedLengthLearningValues.filter({ x => x >= infLastLearningValue })

          // generate features for both dense and sparse learning values
          Seq((fixedLengthLearningValues, "fixed_length"), (fixedTimeLearningValues, "fixed_time")).map({ case (learningValues, learning_values_type) =>
            // generate learning seq and inference value
            val learningSeq = learningValues.map({ x => vs2Map(x) match { case (count, is_anchor_window, post_anchor_flag) => count } })
            val learningSeqAsFloat = learningSeq.map(_.toFloat)
            val (infCountValue, infer_is_anchor_win, infer_post_anchor_flag) = vs2Map(inf_agg_key_value) match { case (count, is_anchor_window, post_anchor_flag) => (count.toFloat,
              is_anchor_window, post_anchor_flag) }

            val (learningMean, learningMedian, learningStdDev, learningMad) = computeStats(learningSeqAsFloat)
            val (learning90Perc, learning95Perc, learningMadAlpha10, learningMadAlpha5) = computePercStats(learningSeqAsFloat)
            val learningStdDevOverMean = (if (learningMean > 0) learningStdDev / learningMean else 0f)
            val learningMadOverMedian = (if (learningMedian > 0) learningMad / learningMedian else 0f)
            val infDiffInStdDev = (if (learningStdDev > 0) ((infCountValue - learningMean) / learningStdDev) else 0f)
            val infDiffInStdDevNonNeg = (if (infDiffInStdDev >= 0) infDiffInStdDev else 0f)
            val infDiffInMad = (if (learningMad > 0) ((infCountValue - learningMedian) / learningMad) else 0f)
            val infDiffInMadNonNeg = (if (infDiffInMad >= 0) infDiffInMad else 0f)
            val infDiffInMad90 = (if (learningMadAlpha10 > 0) (infCountValue - learning90Perc) / learningMadAlpha10 else 0f)
            val infDiffInMad90NonNeg = (if (infDiffInMad90 > 0) infDiffInMad90 else 0f)

            val infDiffInMad95 = (if (learningMadAlpha5 > 0) (infCountValue - learning95Perc) / learningMadAlpha5 else 0f)
            val infDiffInMad95NonNeg = (if (infDiffInMad95 > 0) infDiffInMad95 else 0f)

            // some new metrics. there are 2 classes. 1:
            // mean absolute deviation from the mean

            // 1. ratio from the learningMedian. Used only for strong trends. The stddev is designed as a selection metric centered around 0
            val learningDevFromMedianSeq = learningSeqAsFloat.map({ x => (if (learningMedian > 0) ((x - learningMedian) / learningMedian) else x) })
            val learningDevFromMedianStdDev = computeStats(learningDevFromMedianSeq)._3
            val learningDevFromMedianAbsMean = computeStats(learningDevFromMedianSeq.map({ x => math.abs(x) }))._1
            val infDiffFromMedian = (if (learningMedian > 0) (infCountValue / learningMedian) else infCountValue)

            // 2. relative deviation from the max value seen so far.
            val learningMax = (if (learningSeq.size > 0) learningSeq.max else 0f)
            val infDiffFromMax = (if (learningMax > 0) (infCountValue / learningMax) else infCountValue)

            val kv: Map[String, String] = List(
              ("numFeaturesInInference", "-1"),
              ("learningMean", learningMean.toString),
              ("learningMedian", learningMedian.toString),
              ("learningStdDev", learningStdDev.toString),
              ("learningMad", learningMad.toString),
              ("learningStdDevOverMean", learningStdDevOverMean.toString),
              ("learningMadOverMedian", learningMadOverMedian.toString),
              ("infDiffInStdDevNonNeg", infDiffInStdDevNonNeg.toString),
              ("infDiffInMadNonNeg", infDiffInMadNonNeg.toString),
              ("infDiffInMad90NonNeg", infDiffInMad90NonNeg.toString),
              ("infDiffInMad95NonNeg", infDiffInMad95NonNeg.toString),
              ("learningDevFromMedianAbsMean", learningDevFromMedianAbsMean.toString),
              ("learningDevFromMedianStdDev", learningDevFromMedianStdDev.toString),
              ("infDiffFromMedian", infDiffFromMedian.toString),
              ("learningMax", learningMax.toString),
              ("infDiffFromMax", infDiffFromMax.toString),
              ("numLearningSeq", learningSeq.size.toInt.toString),
              ("infer_is_anchor_win", infer_is_anchor_win.toInt.toString),
              ("infer_post_anchor_flag", infer_post_anchor_flag.toInt.toString),
              ("learningValues", learningValues.mkString(",")),
              ("learningSeq", learningSeq.mkString(",")),
              ("learningDevFromMedianSeq", learningDevFromMedianSeq.mkString(",")),
              ("anchor_ts_str", anchor_ts_str),
              ("event_set", event_set)
            ).toMap

            (id_level1, id_level2, uuid, grouping_level, learning_values_type, event_name, col_name1, col_name2, col_name3, col_value1, col_value2, col_value3,
              cardinality, agg_key_name, learningWindow, learningValues.size, inf_agg_key_value, infCountValue.toInt, infLastLearningValue, kv)
          })
        })
        trendValues
      })

      // create data frame
      val df = spark.createDataFrame(aggTrendValues)
        .toDF("id_level1", "id_level2", "uuid", "grouping_level", "learning_values_type", "event_name", "col_name1", "col_name2", "col_name3", "col_value1", "col_value2", "col_value3",
          "cardinality", "agg_key_name", "learningWindow", "numLearningValues", "inf_agg_key_value", "infCountValue", "infLastLearningValue", "kv")

      // save
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      // drop view
      dict.unpersist()
      spark.catalog.dropTempView("dict")
    }

    def generateTrendsAnalysis(spark: SparkSession, trendsPath: String, outputPath: String, numReducers: Int = 10): Unit = {
      // return if already exists
      if (!true && Utils.checkIfExists(spark, outputPath)) {
        println("generateTrendsAnalysis: output already exists. Skipping... : " + outputPath)
        return
      }

      println("generateTrendsAnalysis: [WARN] : commenting dict and generic types features")
      println("generateTrendsAnalysis: [WARN] : this is generating topk features only if it contains intensity_level0")

      // read all trends
      val trends = spark.read.parquet(trendsPath) //.cache
      trends.createOrReplaceTempView("trends")

      // split the execution based on aids
      Utils.XUUIDS.map({ xuuid =>
        // create output path for the xuuid and check if it exists
        val xuuidOutputPath = outputPath + "/xuuid=" + xuuid
        (xuuid, xuuidOutputPath)
      })
      .filter({ case (xuuid, xuuidOutputPath) => true || Utils.checkIfExists(spark, xuuidOutputPath) == false })
      .foreach({ case (xuuid, xuuidOutputPath) =>
        // create query
        val query = "select id_level1, id_level2, uuid, grouping_level, learning_values_type, event_name, col_name1, col_name2, col_name3, col_value1, col_value2, col_value3, cardinality, " +
          " agg_key_name, inf_agg_key_value, " +
          " kv.infDiffInStdDevNonNeg, kv.infDiffInMadNonNeg, kv.infDiffInMad90NonNeg, kv.infDiffFromMedian, kv.infDiffFromMax, kv.numFeaturesInInference, " +
          " kv.learningMedian, kv.learningMax, kv.learningDevFromMedianAbsMean, kv.learningDevFromMedianStdDev, infCountValue, kv.numLearningSeq, kv.infer_is_anchor_win, " +
          " kv.infer_post_anchor_flag, kv.anchor_ts_str, kv.event_set from trends where id_level1 LIKE '%s%%'".format(xuuid)

        // debug
        println("generateTrendsAnalysis: xuuid query: %s".format(query))

        // get trend values
        val trendsValues = spark.sql(query).rdd.flatMap({ row =>
          val id_level1 = row.getString(row.fieldIndex("id_level1"))
          val id_level2 = row.getString(row.fieldIndex("id_level2"))
          val uuid = row.getString(row.fieldIndex("uuid"))
          val grouping_level = row.getString(row.fieldIndex("grouping_level"))
          val learning_values_type = row.getString(row.fieldIndex("learning_values_type"))
          val event_name = row.getString(row.fieldIndex("event_name"))
          val col_name1 = row.getString(row.fieldIndex("col_name1"))
          val col_name2 = row.getString(row.fieldIndex("col_name2"))
          val col_name3 = row.getString(row.fieldIndex("col_name3"))
          val col_value1 = row.getString(row.fieldIndex("col_value1"))
          val col_value2 = row.getString(row.fieldIndex("col_value2"))
          val col_value3 = row.getString(row.fieldIndex("col_value3"))
          val cardinality = row.getString(row.fieldIndex("cardinality"))
          val agg_key_name = row.getString(row.fieldIndex("agg_key_name"))
          val inf_agg_key_value = row.getString(row.fieldIndex("inf_agg_key_value"))
          val infDiffInStdDevNonNeg = row.getString(row.fieldIndex("infDiffInStdDevNonNeg")).toFloat
          val infDiffInMadNonNeg = row.getString(row.fieldIndex("infDiffInMadNonNeg")).toFloat
          val infDiffInMad90NonNeg = row.getString(row.fieldIndex("infDiffInMad90NonNeg")).toFloat
          val infDiffFromMedian = row.getString(row.fieldIndex("infDiffFromMedian")).toFloat
          val infDiffFromMax = row.getString(row.fieldIndex("infDiffFromMax")).toFloat
          val numFeaturesInInference = row.getString(row.fieldIndex("numFeaturesInInference")).toInt
          val learningMedian = row.getString(row.fieldIndex("learningMedian")).toFloat
          val learningMax = row.getString(row.fieldIndex("learningMax")).toFloat
          val learningDevFromMedianAbsMean = row.getString(row.fieldIndex("learningDevFromMedianAbsMean")).toFloat
          val learningDevFromMedianStdDev = row.getString(row.fieldIndex("learningDevFromMedianStdDev")).toFloat
          val infCountValue = row.getString(row.fieldIndex("infCountValue")).toInt
          val numLearningSeq = row.getString(row.fieldIndex("numLearningSeq")).toInt
          val infer_is_anchor_win = row.getString(row.fieldIndex("infer_is_anchor_win")).toInt
          val infer_post_anchor_flag = row.getString(row.fieldIndex("infer_post_anchor_flag")).toInt
          val anchor_ts_str = row.getString(row.fieldIndex("anchor_ts_str"))
          val event_set = row.getString(row.fieldIndex("event_set"))

          // assign metric
          val (metric, metricType, metricVariable, metricThresh) = {
            if (learningDevFromMedianAbsMean <= 0.5 && learningDevFromMedianStdDev <= 1.0 && numLearningSeq >= MinStrongTrendNumLearningSeq &&
              learningMedian >= MinStrongTrendLearningMedian) {
              // (infDiffFromMedian, "intensity_level0_from_median", learningMedian, MinStrongTrendLearningMedian)
              (infDiffFromMedian, "intensity_level0", learningMedian, MinStrongTrendLearningMedian)
            } else {
              val minEventTypeThreshold = {
                if (event_name == "ANY" && col_name1 == "event_name") {
                  MinWeakDiffFromMaxThreshMap.get(col_value1).getOrElse(1 * MinWeakDiffFromMaxThreshDefault)
                } else {
                  MinWeakDiffFromMaxThreshRatio * MinWeakDiffFromMaxThreshDefault
                }
              }

              // check if it is a anchor
              // TODO: this is not working
              if (event_name == "ANY" && isAnchorEventName(col_value1)) {
                (1f, "anchor", 0f, 1)
              } else {
                // find the min thresholds for metric value for each event type
                if (minEventTypeThreshold > 0 && infDiffFromMax >= minEventTypeThreshold && numLearningSeq >= 1) {
                  (infDiffFromMedian, "intensity_level1", learningMax, minEventTypeThreshold)
                } else if (minEventTypeThreshold > 0 && infCountValue >= minEventTypeThreshold && numLearningSeq == 0) {
                  (infDiffFromMedian, "spike", 0f, minEventTypeThreshold)
                } else {
                  (0f, "", 0f, 0)
                }
              }
            }
          }

          // generate simple dict metrics
          val (dmetric, dmetricType, dmetricVariable, dmetricThresh) = {
            if (infCountValue > 0)
              (infCountValue.toFloat, "dict", 0f, 0)
            else
              (0f, "", 0f, 0)
          }

          // excluded features
          val excludedMetricTypes = Set(
            ("", "")
          )


          // for the time being remove the type of metric
          val metricTypeV2 = {
            if (metricType == "") ""
            else if (ExcludedEventGroupNames.contains(col_value1)) ""
            else if (excludedMetricTypes.contains((col_value1, metricType))) ""
            else "generic"
          }

          // remove some excluded dict columns
          val dmetricTypeV2 = (if (ExcludedEventGroupNames.contains(col_value1)) "" else dmetricType)

          // generate intensity_level0 features separately too
          val metricTypeStrong = (if (metricType == "intensity_level0") metricType else "")

          // val metric = infDiffFromMedian // infDiffInMadNonNeg
          val feature_name = List(event_name, col_name1, col_name2, col_name3, col_value1, col_value2, col_value3, cardinality).mkString(":")

          // generate both diff and dict metrics
          Seq(
            ((id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag),
              (numFeaturesInInference, metric, metricTypeV2, metricVariable, metricThresh, feature_name, infCountValue)),
            ((id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag),
              (numFeaturesInInference, metric, metricTypeStrong, metricVariable, metricThresh, feature_name, infCountValue)),
            ((id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag),
              (numFeaturesInInference, dmetric, dmetricTypeV2, dmetricVariable, dmetricThresh, feature_name, infCountValue))
          )
        })
        .filter({ case ((id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag),
          (numFeaturesInInference, metric, metricType, metricVariable, metricThresh, feature_name, infCountValue)) =>
          // remove this comment to include dict and generic types
          // metricType != ""
          metricType != "" && Set("dict", "generic", "intensity_level1").contains(metricType) == false
        })
        .groupByKey(numReducers)
        .map({ case ((id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag), vs) =>
          // do analysis of the trend in the metric, spotting outliers or generating a graph
          val vs2 = vs.toList
          val TopKValues = 10000
          val MinMetricValue = 2
          val top_k_features = vs2
            .map({ case (numFeaturesInInference, metric, metricType, metricVariable, metricThresh, feature_name, infCountValue) =>
              (feature_name.split(":", -1).filter({ x => x.startsWith("empty") == false && x.trim.length > 0 }).mkString(":"), metric.toInt, metricType, metricVariable.toInt,
                metricThresh.toInt, infCountValue.toInt)
            })
            //.map({ case (numFeaturesInInference, metric, feature_name) =>
            //  (feature_name.split(":", -1).filter({ x => x != "ANY" && x != "event_name" && x.startsWith("empty") == false && x.trim.length > 0 }).mkString(":"), metric.toInt)
            //})
            .map({ case (feature_name, metric, metricType, metricVariable, metricThresh, infCountValue) =>
              (feature_name.replaceAll("ANY:event_name:", ""), metric, metricType, metricVariable, metricThresh, infCountValue)
            })
            .filter({ case (feature_name, metric, metricType, metricVariable, metricThresh, infCountValue) =>
              (feature_name.indexOf("intensity_level0") != -1 && metric >= 1) ||
              (feature_name.indexOf("Level1NodeId") != -1 && metric >= 1) ||
              (feature_name.indexOf("Level2NodeId") != -1 && metric >= 1) ||
              (feature_name.endsWith("unary") && metric >= 1) ||
              metric >= MinMetricValue
            })
            .sortBy({ case (feature_name, metric, metricType, metricVariable, metricThresh, infCountValue) => feature_name })
            .sortBy({ case (feature_name, metric, metricType, metricVariable, metricThresh, infCountValue) => metric })
            .reverse
            .take(math.min(vs2.size, TopKValues))
            .mkString("|")

          val numFeaturesInInferenceStr = vs2
           .map({ case (numFeaturesInInference, metric, metricType, metricVariable, metricThresh, feature_name, infCountValue) => numFeaturesInInference })
           .distinct.toList.mkString(",")

          // sort by the inf_agg_key_value
          val vsMetrics = vs.map({ case (numFeaturesInInference, metric, metricType, metricVariable, metricThresh, feature_name, infCountValue) =>
            val metric_b = (if (metric >= 2) metric else 0f)
            val metric_a = (if (metric >= 1) metric else 0f)
            // infCountValue is not propagated beyond this point
            (metric_b, metric_a, metric, metric / numFeaturesInInference)
          })

          // compute sum and num values in sum
          val (metric_b, metric_b_count, metric_a, metric_a_count, metricSum, metricAvg) = (vsMetrics.map(_._1).sum.toInt, vsMetrics.filter(_._1 > 0).size,
            vsMetrics.map(_._2).sum.toInt, vsMetrics.filter(_._2 > 0).size, vsMetrics.map(_._3).sum, vsMetrics.map(_._4).sum)
          // val metricAvg = vs.map({ case (numFeaturesInInference, infDiffInMad95NonNeg) => infDiffInMad95NonNeg / numFeaturesInInference }).sum

          ((id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str), (inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag, metric_b,
            metric_b_count, metric_a, metric_a_count, metricSum, metricAvg, top_k_features, numFeaturesInInferenceStr))
        })
        .groupByKey(numReducers)
        .flatMap({ case ((id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str), vs) =>
          val vs2 = vs.toList
          vs2
            .sortBy({ case (inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag, metric_b, metric_b_count, metric_a, metric_a_count, metricSum, metricAvg, top_k_features,
              numFeaturesInInferenceStr) => inf_agg_key_value
            })
            .zipWithIndex
            .map({ case ((inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag, metric_b, metric_b_count, metric_a, metric_a_count, metricSum, metricAvg, top_k_features,
              numFeaturesInInferenceStr), index) =>

              val anchorTimestamp = 0L
              val dateTime = new DateTime(anchorTimestamp, DateTimeZone.UTC)
              val dateTimeDateStr = "%04d-%02d-%02d".format(dateTime.getYear(), dateTime.monthOfYear().getAsString().toInt, dateTime.dayOfMonth().getAsString().toInt)
              val dateTimeHourlyStr =   dateTimeDateStr + "-" + "%02d0000".format(dateTime.hourOfDay().getAsString().toInt)
              val dateTimeHourly3Str =  dateTimeDateStr + "-" + "%02d0000".format((dateTime.hourOfDay().getAsString().toInt / 3).toInt * 3)
              val dateTimeHourly6Str =  dateTimeDateStr + "-" + "%02d0000".format((dateTime.hourOfDay().getAsString().toInt / 6).toInt * 6)
              val dateTimeMinute15Str = dateTimeDateStr + "-" + "%02d%02d00".format(dateTime.hourOfDay().getAsString().toInt, (dateTime.minuteOfHour().getAsString().toInt / 15).toInt * 15)
              val dateTimeMinute10Str = dateTimeDateStr + "-" + "%02d%02d00".format(dateTime.hourOfDay().getAsString().toInt, (dateTime.minuteOfHour().getAsString().toInt / 10).toInt * 10)
              val dateTimeMinute5Str = dateTimeDateStr + "-" + "%02d%02d00".format(dateTime.hourOfDay().getAsString().toInt, (dateTime.minuteOfHour().getAsString().toInt / 5).toInt * 5)
              val dateTimeMinute2Str = dateTimeDateStr + "-" + "%02d%02d00".format(dateTime.hourOfDay().getAsString().toInt, (dateTime.minuteOfHour().getAsString().toInt / 2).toInt * 2)
              val dateTimeMinute1Str = dateTimeDateStr + "-" + "%02d%02d00".format(dateTime.hourOfDay().getAsString().toInt, (dateTime.minuteOfHour().getAsString().toInt / 1).toInt * 1)

              // this can be removed later. TODO
              val keyValuesSet = Set(
                ("daily", dateTimeDateStr),
                ("hourly", dateTimeHourlyStr),
                // ("hourly3", dateTimeHourly3Str),
                // ("hourly6", dateTimeHourly6Str),
                // ("minute15", dateTimeMinute15Str)
                ("minute10", dateTimeMinute10Str),
                ("minute05", dateTimeMinute5Str),
                ("minute02", dateTimeMinute2Str),
                ("minute01", dateTimeMinute1Str)
              )

              // different flags
              val valuesSet = Set(dateTimeDateStr, dateTimeHourlyStr, dateTimeHourly3Str, dateTimeHourly6Str, dateTimeMinute15Str)
              val part_of_anchor = (if (infer_post_anchor_flag > 0 && keyValuesSet.contains((agg_key_name, inf_agg_key_value))) 1 else 0)
              val post_anchor_flag = (if (infer_post_anchor_flag > 0 && keyValuesSet.exists({ case (k, v) => k == agg_key_name && v >= inf_agg_key_value })) 1 else 0)
              val pred_win_flag = (if (infer_post_anchor_flag == 0 && keyValuesSet.exists({ case (k, v) => k == agg_key_name && v < inf_agg_key_value })) 1 else 0)
              val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
              val dtStr = fmt.print(dateTime)
              val anchor_time_str = (if (part_of_anchor == 1) dtStr else "")

              // also take only the strong features out of topk for debugging
              val top_k_features_intensity_level0 = top_k_features.split("[|]").filter({ x => x.indexOf("intensity_level0") != -1 || x.indexOf("spike") != -1 }).mkString("|")

              // generate output
              (id_level1, id_level2, uuid, grouping_level, learning_values_type, agg_key_name, anchor_ts_str, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag,
                metric_b, metric_b_count,
                metric_a, metric_b_count, top_k_features, numFeaturesInInferenceStr, part_of_anchor, post_anchor_flag, anchor_time_str, pred_win_flag, top_k_features_intensity_level0)
            })
        })
        .repartition(numReducers)

        // create dataframe
        val df = spark.createDataFrame(trendsValues)
          .toDF("id_level1", "id_level2", "uuid", "grouping_level", "learning_values_type", "agg_key_name", "anchor_ts_str", "inf_agg_key_value", "infer_is_anchor_win", "infer_post_anchor_flag",
            "metric_b", "metric_b_count", "metric_a", "metric_a_count",
            "top_k_features", "numFeaturesInInferenceStr", "part_of_anchor", "post_anchor_flag", "anchor_time_str", "pred_win_flag", "top_k_features_intensity_level0")

        // persist
        df.write.mode(SaveMode.Overwrite).parquet(xuuidOutputPath)
      })

      // create success file at the top level
      Utils.createSuccessFile(spark, outputPath)

      // clear cache
      trends.unpersist()
      spark.catalog.dropTempView("trends")
    }

    def isAnchorEventName(x: String) = {
      x.startsWith("Anchor")
    }

    def generateTrendsDataset(spark: SparkSession, trendsAnalysisPath: String, outputPath: String, numReducers: Int = 10): Unit = {
      // return if already exists
      if (Utils.checkIfExists(spark, outputPath)) {
        println("generateTrendsDataset: output already exists. Skipping... : " + outputPath)
        return
      }

      // read data
      val trendsAnalysis = spark.read.parquet(trendsAnalysisPath)
      trendsAnalysis.createOrReplaceTempView("trends_analysis")

      // debug
      println("generateTrendsDataset: using xminute60 instead of xminute05")

      // create output
      val rdd = spark.sql("select id_level1, id_level2, grouping_level, learning_values_type, agg_key_name, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag, top_k_features, " +
        " anchor_ts_str from trends_analysis" +
        " where (grouping_level like 'level2-%-min10-positive%') AND int(infer_is_anchor_win) >=1 AND int(infer_post_anchor_flag)=1 AND learning_values_type='fixed_length' " +
        " AND top_k_features != '' AND agg_key_name like 'xminute60'").rdd.flatMap({ row =>
        // read
        val id_level1 = row.getString(row.fieldIndex("id_level1"))
        val id_level2 = row.getString(row.fieldIndex("id_level2"))
        val grouping_level = row.getString(row.fieldIndex("grouping_level"))
        val learning_values_type = row.getString(row.fieldIndex("learning_values_type"))
        val agg_key_name = row.getString(row.fieldIndex("agg_key_name"))
        val inf_agg_key_value = row.getString(row.fieldIndex("inf_agg_key_value"))
        val infer_is_anchor_win = row.getString(row.fieldIndex("infer_is_anchor_win")).toFloat.toInt
        val infer_post_anchor_flag = row.getInt(row.fieldIndex("infer_post_anchor_flag"))
        val top_k_features = row.getString(row.fieldIndex("top_k_features"))
        val anchor_ts_str = row.getString(row.fieldIndex("anchor_ts_str"))
        val is_pos = if (grouping_level.endsWith("positive")) 1 else 0
        val is_post_anchor = if (inf_agg_key_value >= anchor_ts_str.replace("T", "-").replace(":", "-")) 1 else 0

        // maximum value to address extreme values
        val MaxFeatureValue = 1000

        // parse top_k_features and generate faeture columns
        val top_k_features_list = top_k_features.split("[|]")
          .map({ t => t.replaceAll("[()]", "") })
          .map({ t => t.split(",").toList
            match {
              case List(event_name, value, feature_type, _, _, _) => (event_name, value.toFloat, feature_type)
            }
          })
          .map({ case (event_name, value, feature_type) => (event_name, math.min(MaxFeatureValue, value), feature_type) })
          .toList

        // output only a sample
        // if (infer_post_anchor_flag == 1) {
        Some((id_level1, id_level2, grouping_level, learning_values_type, agg_key_name, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag, is_pos, is_post_anchor,
          anchor_ts_str, top_k_features_list))
        // } else {
        //   None
        // }
      })

      // get the names
      val all_features_map = rdd.flatMap({ case (id_level1, id_level2, grouping_level, learning_values_type, agg_key_name, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag, is_pos,
        is_post_anchor, anchor_ts_str, top_k_features_list) =>

        val unaryFeatures = top_k_features_list.map({ case (event_name, value, feature_type) => (event_name + "_" + feature_type, 1) })
        val feature_names = top_k_features_list.map({ case (event_name, value, feature_type) => event_name + "_" + feature_type })
        // TODO: Hack
        // val binary_features = feature_names.filter(_.indexOf("unary") != -1).flatMap({ x1 => feature_names.flatMap({ x2 => if (x1 < x2) Some((x1 + "_" + x2, 2)) else None }) })
        // val ternary_features = feature_names.flatMap({ x1 =>
        //   feature_names.flatMap({ x2 => feature_names.flatMap({ x3 => if (x1 < x2 && x2 < x3) Some((x1 + "_" + x2 + "_" + x3, 3)) else None }) })
        // })
        // (unaryFeatures ++ binary_features ++ ternary_features)
        (unaryFeatures)
          .map({ case (feature_name, featureSize) => (feature_name, featureSize, is_pos, id_level2.substring(0, 5)) })
          .toSeq
      })
      .distinct()
      .map({ case (feature_name, featureSize, is_pos, id_level2) => ((feature_name, featureSize, is_pos), Set(id_level2)) })
      .reduceByKey({ case (c1, c2) => c1 ++ c2 }, numPartitions = numReducers)
      .filter({ case ((feature_name, featureSize, is_pos), uniq_level2_ids) =>
        val threshold = (if (is_pos == 1) MinDictFeaturesCountPos else MinDictFeaturesCountNeg)
        if (feature_name.endsWith("_dict")) {
          uniq_level2_ids.size >= threshold
        } else {
          if (feature_name == "intensity_level0") {
            uniq_level2_ids.size >= threshold
          } else {
            uniq_level2_ids.size >= threshold
          }
        }
      })
      .map({ case ((feature_name, featureSize, is_pos), uniq_level2_ids) => ((feature_name, featureSize), uniq_level2_ids) })
      .reduceByKey({ case (c1, c2) => c1 ++ c2 })
      .map({ case ((feature_name, featureSize), uniq_level2_ids) => (feature_name, uniq_level2_ids.size) })
      .collect()
      .toList
      .toMap

      // print
      println("Features Map features: %d".format(all_features_map.size))

      // list
      // val allFeaturesList = all_features_map.keys.toList.sorted
      val allFeaturesList = all_features_map.keys.toSet

      // create broadcast variable
      val all_features_list_bc = spark.sparkContext.broadcast(allFeaturesList)

      // iterate again to create a dense output. spark data frame wont support these many features, so just need to output as lines
      val datasetData = rdd.map({ case (id_level1, id_level2, grouping_level, learning_values_type, agg_key_name, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag,
        is_pos, is_post_anchor, anchor_ts_str, top_k_features_list) =>
        // feature set for different granularity
        val top_k_unary_features_map = top_k_features_list.map({ case (event_name, value, feature_type) => ((event_name + "_" + feature_type), value.toString) }).toMap
        // val topKBinaryFeaturesMap = top_k_features_list.flatMap({ case (event_name1, value1, feature_type1) =>
        //   top_k_features_list.flatMap({ case (event_name2, value2, feature_type2) =>
        //     if (event_name1.indexOf("unary") != -1 && event_name2.indexOf("unary") != -1 && event_name1 < event_name2)
        //       Some((event_name1 + "_" + feature_type1 + "_" + event_name2 + "_" + feature_type2), math.min(value1, value2))
        //     else
        //       None
        //   })
        // })
        // .toMap
        // val top_k_ternary_features_map = top_k_features_list.flatMap({ case (event_name1, value1, feature_type1) =>
        //   top_k_features_list.flatMap({ case (event_name2, value2, feature_type2) =>
        //     top_k_features_list.flatMap({ case (event_name3, value3, feature_type3) =>
        //       if (event_name1 < event_name2 && event_name2 < event_name3) Some((event_name1 + "_" + event_name2 + "_" + event_name3), math.min(math.min(value1, value2), value3)) else None
        //     })
        //   })
        // })
        // .toMap

        // generate features
        // val features = all_features_list_bc.value.map({ case feature_name =>
        //   if (top_k_unary_features_map.contains(feature_name))
        //     top_k_unary_features_map(feature_name)
        //   else
        //     topKBinaryFeaturesMap.get(feature_name).getOrElse(top_k_ternary_features_map.get(feature_name).getOrElse(0f))
        // })
        // val features = all_features_list_bc.value.map({ case feature_name => top_k_unary_features_map.get(feature_name).getOrElse(topKBinaryFeaturesMap.get(feature_name).getOrElse(0f)) })
        // val features = all_features_list_bc.value.map({ case feature_name => "\"%s\": \"%s\"".format(feature_name.replaceAll(":", "_"), top_k_unary_features_map.get(feature_name)) }).flatten
        val features = top_k_unary_features_map.flatMap({ case (k, v) => if (all_features_list_bc.value.contains(k)) Some("\"%s\":\"%s\"".format(k.replaceAll(":", "_"), v)) else None })
        val features_str = Utils.urlEncode("{" + features.mkString(",") + "}")
        (id_level1, id_level2, grouping_level, learning_values_type, agg_key_name, inf_agg_key_value, infer_is_anchor_win, infer_post_anchor_flag, is_pos, is_post_anchor,
          anchor_ts_str, features_str)
        // fields.mkString("\t")
      })
      // .repartition(1)
      // .collect()
      // .toList

      // val featuresHeader = allFeaturesList.map(_.replaceAll(":", "_")).mkString("\t")
      val featuresHeader = "features:json_encoded"
      val df = spark.createDataFrame(datasetData)
        .toDF("id_level1", "id_level2", "grouping_level", "learning_values_type", "agg_key_name", "inf_agg_key_value", "infer_is_anchor_win", "infer_post_anchor_flag",
        "is_pos", "is_post_anchor", "anchor_ts_str", featuresHeader)

      // append header
      // val featuresHeader = allFeaturesList.map(_.replaceAll(":", "_")).mkString("\t")
      // val dataset = List(datasetHeader) ++ datasetData
      df.coalesce(numReducers).repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("sep", "\t").option("quote", "").save(outputPath)

      // parallelize
      // spark.sparkContext.parallelize(dataset, 1).saveAsTextFile(outputPath, classOf[GzipCodec])

      // release broadcast variable
      all_features_list_bc.unpersist()
      spark.catalog.dropTempView("trends_analysis")
    }
}
