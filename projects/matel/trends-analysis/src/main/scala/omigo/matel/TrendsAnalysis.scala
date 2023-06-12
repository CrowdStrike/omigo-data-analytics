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

object TrendsAnalysis {
    val EmptyVal = """'a'"""
    val Empty1 = "empty1"
    val Empty2 = "empty2"
    val Empty3 = "empty3"

    val DefaultAnchorTs = 1577836800L // 2020-01-01
    val DefaultAnchorTsStr = "2020-01-01T00:00:00"

    val SelectedBinaryFeatures = Seq("Feature1", "Feature2")
    val ExcludedEventGroupNames = Seq("Feature3")

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

      // group columns
      def eventMappingFunc(x: String) = x 
      def kvMappingFunc(mp: Map[String, String]): Map[String, String] = mp 
      createGroups(spark, outputDir + "/ancestry", eventMappingFunc, kvMappingFunc, outputDir + "/groups")
   
      // 5. generate different kinds of dictionaries
      createDicts(spark, outputDir + "/groups", outputDir + "/dicts")
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
   
    def createDicts(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
      createDict(spark, inputPath, outputPath, "ANY", EmptyVal, Empty1, EmptyVal, Empty2, EmptyVal, Empty3)
      createDict(spark, inputPath, outputPath, "event_name1", "kv.local_hour", "local_hour", EmptyVal, Empty2, EmptyVal, Empty3)
      createDict(spark, inputPath, outputPath, "event_name1", EmptyVal, Empty1, EmptyVal, Empty2, EmptyVal, Empty3)
      createDict(spark, inputPath, outputPath, "event_name2", EmptyVal, Empty1, EmptyVal, Empty2, EmptyVal, Empty3)
    }

    // FIXME: this is a ugly hack to create some alternate serialization paths 
    case class CreateDictLambdaColValueCond(colName: String, rowValue: String) {
      def getValue() = {
        if (colName == EmptyVal) "" else rowValue
      }
    }

    // Serialized versions of lambda functions
    case class CreateDictLambdaSelectedFeaturesFilter(colValue: String) {
      def getValue() = {
        SelectedBinaryFeatures.contains(colValue) == true && ExcludedEventGroupNames.contains(colValue) == false
      }
    }

    def createDict(spark: SparkSession, inputPath: String, outputPath: String, event_name: String, col_name1: String, col_alias1: String,
      col_name2: String, col_alias2: String, col_name3: String, col_alias3: String): Unit = {
      
      // create output file
      val dictOutputPath = outputPath + "/event_name=" + event_name + "/dcol_name1=" + col_alias1 + "/dcol_name2=" + col_alias2 + "/dcol_name3=" + col_alias3
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

        val col_value1 = CreateDictLambdaColValueCond(col_name1, Utils.convertNullToEmptyString(row.getString(6))).getValue()
        val col_value2 = CreateDictLambdaColValueCond(col_name2, Utils.convertNullToEmptyString(row.getString(7))).getValue()
        val col_value3 = CreateDictLambdaColValueCond(col_name3, Utils.convertNullToEmptyString(row.getString(8))).getValue()

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
      .groupByKey(1000)
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

        // run binary features only for LxPid
        // generate binary features
        if (true) {
          val binaryFeatures = for {
            ((col_value1_1, col_value2_1, col_value3_1, count_1), index_1) <- vs2.zipWithIndex
            ((col_value1_2, col_value2_2, col_value3_2, count_2), index_2) <- vs2.zipWithIndex
            if (col_value1_1 < col_value1_2 && col_value2_1 == col_value2_2 && col_value3_1 == col_value3_2)
          } yield {
            buffer.append((col_value1_1 + "_" + col_value1_2, col_value2_1, col_value3_1, "binary", Seq(count_1, count_2).min))
          }

          val vs2Selected = vs2.filter({ case (col_value1, col_value2, col_value3, count) =>
            // SelectedBinaryFeatures.contains(col_value1) == true && ExcludedEventGroupNames.contains(col_value1) == false
            CreateDictLambdaSelectedFeaturesFilter(col_value1).getValue() == true
          })

          // val vs2Selected = vs2
          val ternaryFeatures = for {
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
}
