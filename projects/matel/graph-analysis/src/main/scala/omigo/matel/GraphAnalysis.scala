package omigo.matel 

import collection.JavaConverters._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.net.URI
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.BitSet
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder,Encoders}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf, SparkFiles}
import org.apache.spark.broadcast.Broadcast
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import scala.reflect.ClassTag
import scala.util.Random

object JoinTypes {
  val tables = List("table1", "table2", "table3")
}

object QueryAnnotation {
  val NodeAgent = "NodeAgent"
  val Action = "Action"
  val Annotation = "Annotation"
}
 
/**
 * Abstract class QueryNode to be part of a query graph.
 */
abstract class QueryNode(val tableName: String, val defaultSelect: Seq[String], val supportedJoinKeys: Seq[String], val defaultJoinKeys: Seq[String],
  val availableFields: Seq[String], val markers: Set[String], val joinKeysMap: Map[String, String], val indexTableName: String) {

  // alias name
  var name = tableName

  // spark session
  var spark: SparkSession = null

  // default join keys
  var joinKeys: Seq[String] = defaultJoinKeys

  // prefix for creating batches
  var batchHashKey = "id1"
  var batchHashPrefix = ""
  var batchWorkingDir = ""
  var batchInputDir = ""

  // select variables
  var selectStringList = new scala.collection.mutable.ListBuffer[String]
  var selectBitSetList = new scala.collection.mutable.ListBuffer[String]

  // self filter 
  var filterStringMap = new scala.collection.mutable.HashMap[String, Set[String]]
  var filterStringSuffixMap = new scala.collection.mutable.HashMap[String, Set[String]]

  // bitset filters for optimized search
  var filterBitSetOrMap = new scala.collection.mutable.HashMap[String, Seq[BitSet]]
  var filterBitSetAndMap = new scala.collection.mutable.HashMap[String, BitSet]

  // pair filters
  var filterStringPairInitialMap = new scala.collection.mutable.HashMap[(String, String), Set[String]]
  var filterStringPairMap = new scala.collection.mutable.HashMap[(String, String), Set[String]]
  var filterNumericLimitPairInitialMap = new scala.collection.mutable.HashMap[(String, String), Int]
  var filterNumericLimitPairMap = new scala.collection.mutable.HashMap[(String, String), Int]

  // context map filter
  var filterContextMapEqualityInitialSet = new scala.collection.mutable.HashSet[String]
  var filterContextMapEqualityMap = new scala.collection.mutable.HashMap[String, String]

  // aggregation filter after the join
  var filterGroupSizeMinLimit = -1
  var filterGroupSizeMaxLimit = -1

  // prev step name
  var prevNodeName = ""
  var prevNodeAgentNodeName = ""
  var prevNodeMarkers: Set[String] = null

  // prev node with selected keys
  var prevNodesWithAvailableKeys = new scala.collection.mutable.HashMap[String, String]

  // start / stop node markers
  var isStartNode = false
  var isEndNode = false

  // add the list of default selected keys to the select clause
  defaultSelect.foreach({ x => addSelectString(x) })

  // base prefixes
  var id1_base_prefix = ""
  var id2_base_prefix = ""

  // methods
  def getName(): String = {
    name
  }

  def setName(x: String): QueryNode = {
    name = x
    this
  }

  def getTableName(): String = {
    tableName
  }

  def setId1BasePrefix(x: String): QueryNode = {
    id1_base_prefix = x
    this
  }

  def setId2BasePrefix(x: String): QueryNode = {
    id2_base_prefix = x
    this
  }

  def getJoinKeyMap(x: String): String = {
    joinKeysMap.get(x).getOrElse(x)
  } 

  def getSelectStringList(): List[String] = {
    selectStringList.toList
  }

  def getSelectBitSetList(): List[String] = { 
    selectBitSetList.toList
  }

  def getPrevNodeName(): String = {
    prevNodeName
  }

  def getMarkers(): Set[String] = {
    markers
  }

  def setPrevNodeMarkers(x: Set[String]): QueryNode = {
    prevNodeMarkers = x
    this
  }

  def getPrevNodeMarkers(): Set[String] = {
    prevNodeMarkers
  }

  def setPrevNodeName(x: String): QueryNode = {
    prevNodeName = x
    this
  }

  def getPrevNodeAgentNodeName(): String = {
    prevNodeAgentNodeName
  }

  def setPrevNodeAgentNodeName(x: String): QueryNode = {
    prevNodeAgentNodeName = x
    this
  }

  def getPrevNodesWithAvailableKeys(): Map[String, String] = {
    prevNodesWithAvailableKeys.toMap
  }

  def getPrevNodeNameWithAvailableKey(key: String): String = {
    println("getPrevNodeNameWithAvailableKey: %s, %s, %s".format(getName(), key, prevNodesWithAvailableKeys))
    prevNodesWithAvailableKeys(key)
  }

  def setPrevNodeNameWithAvailableField(key: String, x: String): QueryNode = {
    println("setPrevNodeNameWithAvailableField: key: %s, x: %s".format(key, x))
    prevNodesWithAvailableKeys.put(key, x)
    this
  }

  def getAvailableFields(): Seq[String] = {
    availableFields
  }

  def setSparkSession(x: SparkSession): QueryNode = {
    spark = x
    this
  }

  def setJoinKeys(keys: Seq[String]): QueryNode = {
    joinKeys = keys
    this
  }

  def getJoinKeys(): Seq[String] = {
    joinKeys
  }

  def setBatchHashKey(x: String): QueryNode = {
    batchHashKey = x
    this
  }

  def setBatchHashPrefix(x: String): QueryNode = {
    batchHashPrefix = x
    this
  }

  def setBatchWorkingDir(x: String): QueryNode = {
    batchWorkingDir = x
    this
  }

  def generateBatchDirectory(baseDir: String, prefix: String, nodeName: String): String = {
    if (prefix != "")
      "%s/node=%s/batch=%s".format(baseDir, nodeName, prefix)
    else
      "%s/node=%s/batch=%s".format(baseDir, nodeName, "ALL")
  }

  def getBatchOutputDir(): String = {
    generateBatchDirectory(batchWorkingDir, batchHashPrefix, getName())
  }

  def addSelectString(name: String): QueryNode = {
    if (selectStringList.contains(name))
      println("addSelectString: key already exists: %s, %s".format(name, selectStringList.toString))
    else
      selectStringList.append(name)

    this
  }

  def addSelectBitSet(name: String): QueryNode = {
    if (selectBitSetList.contains(name))
      println("addSelectBitSet: key already exists: %s, %s".format(name, selectBitSetList.toString))
    else
      selectBitSetList.append(name)

    this
  }
 
  def addFilter(name: String, values: Set[String]): QueryNode = {
    filterStringMap.put(name, values)
    this
  }
 
  def addSuffixFilter(name: String, values: Set[String]): QueryNode = {
    filterStringSuffixMap.put(name, values)
    this
  }

  def addPairValuesFilter(x: String, key: String, values: Set[String]): QueryNode = {
    filterStringPairInitialMap.put((x, key), values)
    this
  }

  def addPairNumericLimitFilter(x: String, key: String, value: Int): QueryNode = {
    filterNumericLimitPairInitialMap.put((x, key), value)
    this
  }

  def addBitSetOrFilter(name: String, values: Set[String]): QueryNode = {
    addSelectBitSet(name)
    filterBitSetOrMap.put(name, values.toSeq.map({ x => constructWordsHash(Seq(x)) }))
    this
  }

  def addBitSetAndFilter(name: String, values: Set[String]): QueryNode = {
    addSelectBitSet(name)
    filterBitSetAndMap.put(name, constructWordsHash(values.toSeq))
    this
  }

  def addContextMapEqualityFilter(key: String): QueryNode = {
    filterContextMapEqualityInitialSet.add(key)
    this
  }

  def getFilterBitSetAndMap(): Map[String, BitSet] = {
    filterBitSetAndMap.toMap
  }

  def getFilterBitSetOrMap(): Map[String, Seq[BitSet]] = {
    filterBitSetOrMap.toMap
  }

  def getFilterStringPairMap(): Map[(String, String), Set[String]] = {
    filterStringPairMap.toMap
  }

  def getFilterNumericLimitPairMap(): Map[(String, String), Int] = { 
    filterNumericLimitPairMap.toMap
  }

  def getFilterContextMapEqualityInitialSet(): Set[String] = {
    filterContextMapEqualityInitialSet.toSet
  }

  def getFilterContextMapEqualityMap(): Map[String, String] = {
    filterContextMapEqualityMap.toMap
  }

  def time_window(limit: Int, source: String = ""): QueryNode = {
    addPairNumericLimitFilter(source, "ts", limit * 60 * 1000)
  }

  def setFilterGroupSizeMinLimit(x: Int): QueryNode = {
    filterGroupSizeMinLimit = x
    this
  }

  def getFilterGroupSizeMinLimit(): Int = {
    filterGroupSizeMinLimit
  }

  def getIsStartNode(): Boolean = {
    isStartNode
  }

  def setIsStartNode(x: Boolean): QueryNode = {
    isStartNode = x
    this
  }

  def getIsEndNode(): Boolean = {
    isEndNode
  }

  def setIsEndNode(x: Boolean): QueryNode = {
    isEndNode = x
    this
  }

  def build(): QueryNode = {

    // transforms the initial pair maps
    filterStringPairInitialMap.foreach({ case ((x, key), value) =>
      if (x == "")
        filterStringPairMap.put((getPrevNodeAgentNodeName(), key), value)
      else
        filterStringPairMap.put((x, key), value)
    })

    // numeric
    filterNumericLimitPairInitialMap.foreach({ case ((x, key), value) =>
      if (x == "")
        filterNumericLimitPairMap.put((getPrevNodeAgentNodeName(), key), value)
      else
        filterNumericLimitPairMap.put((x, key), value)
    })

    // context map
    filterContextMapEqualityInitialSet.foreach({ k => filterContextMapEqualityMap.put(k, getPrevNodeNameWithAvailableKey(k)) })

    this
  }

  override def toString(): String = {
    val nodeDetails = "tableName: %s, defaultSelect: %s, supportedJoinKeys: %s, defaultJoinKeys: %s, joinKeys: %s, batchHashPrefix: %s, prevNodeName: %s," + 
      "prevNodeAgentNodeName: %s, isStartNode: %s, isEndNode: %s".format(tableName, defaultSelect.toString, supportedJoinKeys.toString, defaultJoinKeys.toString,
      joinKeys.toString, batchHashPrefix.toString, prevNodeName, prevNodeAgentNodeName, getIsStartNode().toString, getIsEndNode().toString)
    val availableDetails = "%s".format(availableFields.toString)
    val selectDetails = "selectStringList: %s, selectBitSetList: %s".format(getSelectStringList(), getSelectBitSetList())
    val filterDetails = "filterStringMap: %s, filterBitSetOrMap: %s, filterBitSetAndMap: %s".format(filterStringMap.toString, filterBitSetOrMap.toString, filterBitSetAndMap.keys.toString)
    val filterPairDetails = "filterStringPairInitialMap: %s, filterStringPairMap: %s, filterNumericLimitPairInitialMap: %s, filterNumericLimitPairMap: %s".format(
      filterStringPairInitialMap.toString, filterStringPairMap.toString, filterNumericLimitPairInitialMap.toString, filterNumericLimitPairMap.toString)
    val filterContextMapEqualityDetails = "filterContextMapEqualityInitialSet: %s, filterContextMapEqualityMap: %s".format(filterContextMapEqualityInitialSet, filterContextMapEqualityMap)
    val filterGroupSizeDetails = "filtergroupsize: filterGroupSizeMinLimit: %d, filterGroupSizeMaxLimit: %d".format(filterGroupSizeMinLimit, filterGroupSizeMaxLimit)
    "name: %s | details: %s | available: %s, select: %s | filter : %s, filterpair: %s, filtercontextpairequality: %s, filtergroupsize: %s".format(name, nodeDetails,
      availableDetails, selectDetails, filterDetails, filterPairDetails, filterContextMapEqualityDetails, filterGroupSizeDetails)
  }

  // FIXME
  def getResultCount(): Long = {
    if (isSuccessFileExists(getBatchOutputDir())) {
      spark.read.parquet(getBatchOutputDir()).count
    } else {
      throw new Exception("getResultCount: step didnt finish properly: %s".format(getName()))
    }
  }

  def isSuccessFileExists(path: String): Boolean = {
    CIDUtils.checkIfExists(spark, path)
  }

  // TODO: this fs creation is not same as CIDUtils
  def createSuccessFile(path: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(new Path(path))
    fs.create(new Path(path + "/_SUCCESS")).close()
  }

  def executeAndGetIndexedPaths(): String = {
    // store the results of all the paths in output directory
    val outputDir = generateBatchDirectory(batchWorkingDir, batchHashPrefix, getName() + "-indexed")
    println("executeAndGetIndexedPaths: %s, batch: %s, indexed path: %s".format(getName(),  batchHashPrefix, outputDir))

    // FIXME: Check if the result already exists for all batches
    if (isSuccessFileExists(outputDir)) {
      // found cached execution
      return outputDir
    }

    // generate select fields. FIXME: id1FilterStr is a hack
    val id1FilterStr = {
      if (batchHashKey == "id1") "id1 like '%s%%'".format(id1_base_prefix + batchHashPrefix)
      else if (id1_base_prefix.length > 0) "id1 like '%s%%'".format(id1_base_prefix) else ""
    }
    val selectStr = (filterBitSetAndMap.keys ++ filterBitSetOrMap.keys ++ Seq("input_file_name")).toList.mkString(", ").trim()
    val query = "select %s from %s where %s".format(selectStr, indexTableName, id1FilterStr)
    println("executeAndGetIndexedPaths: %s: batch: %s, query: %s".format(getName(), batchHashPrefix, query))

    // initialize the bitset function
    val bitSetFilterFunctions = new BitSetFilterFunctions(spark.sparkContext.broadcast(getFilterBitSetAndMap()), spark.sparkContext.broadcast(getFilterBitSetOrMap()))

    // execute the indexed query
    val df = spark.sql(query).filter(bitSetFilterFunctions).select(col("input_file_name"))

    // unpersist 
    bitSetFilterFunctions.unpersist()

    // persist for scalability
    df.repartition(10).write.mode("overwrite").parquet(outputDir) 
    
    // return the path of the output directory
    outputDir
  } 

  def generateSelectQuery(inputTableName: String): String = {
    // id1 filter 
    val id1FilterStr = {
      if (batchHashKey == "id1") "id1 like '%s%%'".format(id1_base_prefix + batchHashPrefix)
      else if (id1_base_prefix.length > 0) "id1 like '%s%%'".format(id1_base_prefix)
      else ""
    }

    // id2 filter
    val id2FilterStr = {
      if (batchHashKey == "id2") "id2 like '%s%%'".format(id2_base_prefix + batchHashPrefix)
      else if (id2_base_prefix.length > 0) "id2 like '%s%%'".format(id2_base_prefix)
      else ""
    }

    // select string
    val selectStr = (getSelectStringList() ++ getSelectBitSetList()).mkString(", ")

    // filter string
    val filtersStr = {
      if (filterStringMap.nonEmpty) filterStringMap.map({ case (k, v) => "%s in (%s)".format(k, v.map({ v1 => "'%s'".format(v1) }).toList.sorted.mkString(",")) }).mkString(" and ")
      else ""
    }

    // suffix filter string
    val filtersSuffixStr = {
      if (filterStringSuffixMap.nonEmpty) filterStringSuffixMap.flatMap({ case (k, vs) => vs.map({ v => "%s like '%%%s'".format(k, v) }) }).mkString(" or ")
      else ""
    }

    // filter string combined
    val filtersStrCombined = List(filtersStr, filtersSuffixStr, id1FilterStr, id2FilterStr).filter(_.trim.length > 0).map({ x => "(%s)".format(x) }).mkString(" and ").trim()

    // query
    val query = "select %s from %s where %s".format(selectStr, inputTableName, filtersStrCombined)
    println("Name: %s: Query: %s".format(getName(), query))
    query 
  }

  def getEmptyBatchInput(): BatchInputV2 = {
    val emptyBatchInputList: List[(String, String, String, String, String)] = List.empty
    new BatchInputV2(spark.createDataFrame(spark.sparkContext.parallelize(emptyBatchInputList)).toDF("id1", "id2", "uuid", "node_id", "parent_id"))
  }

  def readPrevNodeOutput(): BatchInputV2 = {
    if (getIsStartNode()) {
      // create empty input
      getEmptyBatchInput()
    } else {
      // read the output of previous node 
      val prevNodeOutputDir = generateBatchDirectory(batchWorkingDir, batchHashPrefix, getPrevNodeName())
      println("Node: %s, prevNode: %s, prevNodeOutputDir: %s".format(getName(), getPrevNodeName(), prevNodeOutputDir))
      new BatchInputV2(spark.read.parquet(prevNodeOutputDir))
    }
  }

  def generateJoinQuery(inputTable: String, batchInputTable: String): String = {
    // get the select clauses for the current input table
    val inputTableSelectString = (getSelectStringList() ++ getSelectBitSetList()).map({ x => "%s.%s as %s_%s".format(inputTable, x, getName(), x) }).mkString(", ").trim()
   
    if (getIsStartNode()) {
      val query = "select %s from %s".format(inputTableSelectString, inputTable)
      println("generateJoinQuery: %s".format(query))
      query
    } else {
      val pnodeName = getPrevNodeAgentNodeName() 
      val joinKeys2 = Seq("id1", "id2") ++ getJoinKeys()
      val joinClauseString = joinKeys2.map({ x => "%s.%s = %s.%s_%s".format(inputTable, x, batchInputTable, pnodeName, getJoinKeyMap(x)) }).mkString(" and ").trim()
      val contextJoinClauseString = getFilterContextMapEqualityMap().toList
        .map({ case (key, pnode) => "%s.%s = %s.%s_%s".format(inputTable, key, batchInputTable, pnode, key) }).mkString(" and ").trim()
      val combinedJoinedString = Seq(joinClauseString, contextJoinClauseString).filter(_.size > 0).mkString(" and ").trim()
      val whereNumericClauseFilterString = (if (getFilterNumericLimitPairMap().nonEmpty) getFilterNumericLimitPairMap().toList
        .map({ case ((nname, key), value) => "%s.%s <= %s.%s_%s + %d".format(inputTable, key, batchInputTable, nname, key, value) }).mkString(" and ").trim() else "")
      val whereTimestampClauseFilterString = (if (getMarkers().contains(QueryAnnotation.Annotation)) "%s.%s_ts >= %s.ts and %s.%s_ts < %s.ts_end".format(batchInputTable,
        pnodeName, inputTable, batchInputTable, pnodeName, inputTable) else "")
      val whereClauseCombinedString = Seq(whereNumericClauseFilterString, whereTimestampClauseFilterString).filter(_.length > 0).mkString(" and ").trim() 
      val whereClauseString = (if (whereClauseCombinedString.length > 0) "where %s".format(whereClauseCombinedString) else "")
      println("inputTableSelectString: %s, joinClauseString: %s, contextJoinClauseString: %s, combinedJoinedString: %s, whereNumericClauseFilterString: %s" +
        ", whereTimestampClauseFilterString: %s, whereClauseCombinedString: %s, whereClauseString: %s".format(inputTableSelectString, joinClauseString, contextJoinClauseString,
        combinedJoinedString, whereNumericClauseFilterString, whereTimestampClauseFilterString, whereClauseCombinedString, whereClauseString))
      val query = "select %s, %s.* from %s join %s on %s %s".format(inputTableSelectString, batchInputTable, inputTable, batchInputTable, combinedJoinedString, whereClauseString)
      println("generateJoinQuery: %s".format(query))
      query
    }
  }

  def hasBitSetFilters(): Boolean = {
    filterBitSetAndMap.nonEmpty || filterBitSetOrMap.nonEmpty
  }

  def hasIndexedBitSetFilters(): Boolean = {
    indexTableName != ""
  }

  /**
   * returns null when the data frame in the path is empty.
   */
  def readDataFrameMultiplePaths(path: String, batchSize: Int): DataFrame = {

    // read the dataframe holding all the locations of different input_file_name
    val indexedPathsDF = spark.read.parquet(path).cache()

    // number of input files
    val numFiles = indexedPathsDF.count
    val numBatches = math.ceil(1.0 * numFiles / batchSize).toInt

    println("readDataFrameMultiplePaths: %s, batch: %s, reading path: %s, batchSize: %d, numFiles: %d, numBatches: %d".format(getName(), batchHashPrefix, path,
      batchSize, numFiles, numBatches))
  
    // check for no match
    if (numFiles == 0) {
      return null
    } else {
      // iterate
      val dfList = Range(0, numBatches).map({ batchNumber => 
        // create the filter
        val inputFileNameBatchFilter = new InputFileNameBatchFilter(spark.sparkContext.broadcast(numBatches), spark.sparkContext.broadcast(batchNumber))
   
        // apply the filter to get the batches
        val indexedPathsDFBatch = indexedPathsDF.filter(inputFileNameBatchFilter)
   
        // unpersist
        inputFileNameBatchFilter.unpersist()
   
        // read the filenames
        val inputFileNames = indexedPathsDFBatch.select(col("input_file_name")).rdd.map(_.getString(0)).collect().toList
        println("readDataFrameMultiplePaths: reading paths: %d".format(inputFileNames.size))
   
        // read the parquet files
        spark.read.parquet(inputFileNames: _*)
      })
   
      // unpersist
      indexedPathsDF.unpersist()
      
      // do a union of all dataframes
      dfList.reduceLeft({ (a, b) => a.union(b) })
    }
  }

  /**
   * This method is to apply all the bitset filter in the sql query execution.
   */
  def executeOptSelectQuery(): DataFrame = {
    // check if this query needs filter on bitset
    if (hasBitSetFilters()) {
      // initialize and broadcast custom bitset filters
      val bitSetFilterFunctions = new BitSetFilterFunctions(spark.sparkContext.broadcast(getFilterBitSetAndMap()), spark.sparkContext.broadcast(getFilterBitSetOrMap()))

      // get the result dataframe after applying the bitset filters
      val dfBitSetResult = {
        if (hasIndexedBitSetFilters()) {

          // get the result of query in a temp table
          val indexedQueryPathsResult = executeAndGetIndexedPaths()
          println("executeOptSelectQuery: %s, batch: %s, indexedQueryPathsResult: %s".format(getName(), batchHashPrefix, indexedQueryPathsResult))

          // optimizing reading of indexed paths in dataframes as there can be too many paths
          val indexedQueryDF = readDataFrameMultiplePaths(indexedQueryPathsResult, 2000)

          // FIXME: check for null condition
          if (indexedQueryDF == null) {
            null
          } else {
            // load the paths in a temp table
            val indexedQueryTempTable = "CURR_%s_%s_indexed".format(getName(), batchHashPrefix)

            // create temp table
            indexedQueryDF.createOrReplaceTempView(indexedQueryTempTable)
            println("executeOptSelectQuery: %s: batch: %s, indexed query temp table: %s, %s".format(getName(), batchHashPrefix, indexedQueryPathsResult, indexedQueryTempTable))
            
            // generate the sql query based on temp table
            val df = spark.sql(generateSelectQuery(indexedQueryTempTable)).filter(bitSetFilterFunctions)

            // unpersist the temp table
            spark.catalog.dropTempView(indexedQueryTempTable)

            // return the result
            df
          }
        } else {
          spark.sql(generateSelectQuery(tableName)).filter(bitSetFilterFunctions)
        }
      }

      // unpersist
      bitSetFilterFunctions.unpersist()
 
      dfBitSetResult
    } else {
      // no bit set filters. generate normal sql query and return the result
      spark.sql(generateSelectQuery(tableName))
    }
  }

  def execute(): Unit = {
    println("execute: %s".format(getName()))

    // read input
    val batchInput = readPrevNodeOutput()
   
    // check if there is any need to execute this step
    if (getIsStartNode() == false && batchInput.getCount() == 0) {
      // create empty output for the batch
      // createSuccessFile(getBatchOutputDir())
      println("Name: %s, empty batch input. skipping the step and writing empty output.".format(getName()))
      getEmptyBatchInput().getDataFrame().write.mode("overwrite").parquet(getBatchOutputDir())
    } else {
      // create temp tables
      val batchInputTempTable = "PREV_%s_%s".format(getPrevNodeAgentNodeName(), batchHashPrefix)
      batchInput.getDataFrame().createOrReplaceTempView(batchInputTempTable)

      // run the select for the current node
      val input = new BatchInputV2(executeOptSelectQuery())

      // check if this was null entry
      if (input.df != null) {
        val inputTempTable = "CURR_%s_%s".format(getName(), batchHashPrefix)
        input.getDataFrame().createOrReplaceTempView(inputTempTable)

        // construct the join query
        val joinedQuery = generateJoinQuery(inputTempTable, batchInputTempTable)
        val joined = spark.sql(joinedQuery)

        // println("execute: %s: input table count: %d".format(getName(), input.getCount()))
        // println("execute: %s: batch input table count: %d".format(getName(), batchInput.getCount()))
        // println("execute: %s: joined count: %d".format(getName(), joined.count()))
 
        // apply the time window filter
        val joinedFilter1 = {
          if (getFilterGroupSizeMinLimit() >= 0) {
            val jkeys = (Seq("id1", "id2") ++ getJoinKeys()).map({ x => "%s_%s".format(getName(), x) }).toSeq
            val selGroups = joined.groupBy(jkeys.head, jkeys.tail: _*).count().filter(col("count") >= getFilterGroupSizeMinLimit()).select(jkeys.head, jkeys.tail: _*)
            joined.join(selGroups, jkeys, "inner")
          } else {
            joined
          }
        }

        println("Node: %s, batch: %s: joinedFilter1 : %d".format(getName(), batchHashPrefix, joinedFilter1.count))

        // apply the context map equality filter
        val joinedFilter2 = {
          joinedFilter1
        }

        // convert to canonical dataframe
        val joinedFilterFinal = {
          joinedFilter2
        }

        // persist
        joinedFilterFinal.write.mode("overwrite").parquet(getBatchOutputDir())
     
        // unpersist temp table 
        spark.catalog.dropTempView(inputTempTable)
      } else {
        // input was null. write empty
        getEmptyBatchInput().df.write.mode("overwrite").parquet(getBatchOutputDir())
      } 
      
      // drop the temporary table
      spark.catalog.dropTempView(batchInputTempTable)
    }
  }
}

/**
 * Pillar for Table1 
 */
class Table1 extends QueryNode(tableName = "table1", defaultSelect = Seq("id1", "id2", "uuid", "node_id", "event_id", "ts"),
  supportedJoinKeys = Seq("uuid"), defaultJoinKeys = Seq("node_id"),
  availableFields = Seq("event_id"), markers = Set(QueryAnnotation.NodeAgent), joinKeysMap = Map.empty, indexTableName = "table1_index") {

  def as(x: String): Table1 = {
    setName(x)
    this
  }
}

/**
 * Pillar for parent child relationship
 */
class Hierarchy extends QueryNode(tableName = "hierarchy", defaultSelect = Seq("id1", "id2", "node_id", "parent_id", "event_id", "ts"),
  supportedJoinKeys = Seq("parent_id"), defaultJoinKeys = Seq("parent_id"), availableFields = Seq("event_id"), markers = Set(QueryAnnotation.NodeAgent),
  joinKeysMap = Map("parent_id" -> "node_id"), indexTableName = "") {

  var inner_time_window_limit = -1 
  var inner_min_group_size_input = -1

  def as(x: String): Hierarchy = {
    setName(x)
    this
  }

  def min_group_size(x: Int): Hierarchy = {
    inner_min_group_size_input = x
    super.setFilterGroupSizeMinLimit(inner_min_group_size_input)
    this
  }

  override def time_window(limit: Int, source: String = ""): Hierarchy = {
    inner_time_window_limit = limit
    super.time_window(limit, source)
    this
  }
}

case class QueryGraphInput(val spark: SparkSession, val workingBaseDir: String, val samplingDepth: Int, val maxResults: Int, val batchKey: String,
  val id1BasePrefix: String, val id2BasePrefix: String)

/**
 * QueryGraph.
 */
class QueryGraph(val name: String, val nodes: Seq[QueryNode], val graphInput: QueryGraphInput) {
  println("QueryGraph: name: %s, graphInput: %s".format(name, graphInput.toString))

  def build(): Unit = {
    // get a hashmap view of node names to node
    val nodesToMap = nodes.map({ node => (node.getName(), node) }).toMap

    // set the previous and next nodes
    Range(1, nodes.size).foreach({ index =>
      // get the current node
      val curNode = nodes(index)

      val prevNodeName = nodes(index - 1).getName()
      val prevNodeMarkers = nodes(index - 1).getMarkers()
      val prevNodeAgentNodeName = nodes.take(index).reverse.find({ node => node.markers.contains(QueryAnnotation.Annotation) == false }).map(_.getName()).getOrElse("")

      // setting the names
      curNode.setPrevNodeName(prevNodeName)
      curNode.setPrevNodeMarkers(prevNodeMarkers)
      curNode.setPrevNodeAgentNodeName(prevNodeAgentNodeName)

      // fill all the fields available till now (from last non annotation node till prev node)
      val prevNonAnnotationNodeIndex = nodes
        .take(index)
        .zipWithIndex
        .reverse
        .find({ case (node, i) => node.markers.contains(QueryAnnotation.Annotation) == false })
        .map({ case (node, i) => i })
        .getOrElse(-1)
      // println("current index: %d, node: %s, prevNonAnnotationNodeIndex: %d".format(index, curNode.getName(), prevNonAnnotationNodeIndex))

      // fill the map for which node to consult for matching which key
      if (prevNonAnnotationNodeIndex != -1) {
        nodes.slice(prevNonAnnotationNodeIndex, index).foreach({ prevNode =>
          // foreach is important as the most recent ones will override the older ones
          prevNode
            .getAvailableFields()
            .foreach({ key => curNode.setPrevNodeNameWithAvailableField(key, prevNode.getName()) })
        })
      }

      // print after changing the prev nodes with available keys
      // println("curindex: %d, curnode: %s, prev non ann: %d,  prevNode.getPrevNodesWithAvailableKeys: %s".format(index, curNode.getName(),
      //   prevNonAnnotationNodeIndex, curNode.getPrevNodesWithAvailableKeys()))
    })
      
    // also add the newly available keys required for join into the select clause
    nodes.foreach({ node => node.getFilterContextMapEqualityInitialSet().map({ k => nodesToMap(node.getPrevNodeNameWithAvailableKey(k)).addSelectString(k) }) })

    // start / end nodes
    nodes.head.setIsStartNode(true)
    nodes.last.setIsEndNode(true)

    // call build on all
    nodes.foreach({ node => node.build() })
  }

  def isPrimitiveKey(key: String) = {
    Set("id1", "id2", "node_id", "uuid", "event_id", "ts", "ts_end").contains(key)
  }

  def isId1Id2Key(key: String) = {
    Set("id1", "id2").contains(key)
  }

  def execute(): Unit = {
    println("Graph : %s, workingBaseDir: %s".format(name, graphInput.workingBaseDir))

    // iterate over all the nodes. Check for input from previous step and create the necessary context
    val batchHashPrefixes = {
      graphInput.samplingDepth match {
        case 0 => Seq("")
        case 1 => UUID_PREFIXES.map(_.toString).toSeq
        case 2 => UUID_PREFIXES.flatMap({ xid11 => UUID_PREFIXES.map({ xid12 => "" + xid11 + xid12 }) }).toSeq
        case _ => throw new Exception("samplingDepth: %d not supported".format(graphInput.samplingDepth))
      }
    }

    var totalResults = 0L
    var batchesExecuted = 0
  
    // set the batch prefix for every node
    batchHashPrefixes.foreach({ batchHashPrefix =>
      if (totalResults < graphInput.maxResults) {
        var prevResultsDir: String = null
        Range(0, nodes.size).foreach({ curIndex =>
          // get current and previous node
          val curNode = nodes(curIndex)
          val prevNode = (if (curIndex > 0) nodes(curIndex - 1) else null)

          // set the batch prefix
          curNode.setSparkSession(graphInput.spark)
          curNode.setBatchHashKey(graphInput.batchKey)
          curNode.setId1BasePrefix(graphInput.id1BasePrefix)
          curNode.setId2BasePrefix(graphInput.id2BasePrefix)
          curNode.setBatchWorkingDir(graphInput.workingBaseDir)
          curNode.setBatchHashPrefix(batchHashPrefix)

          // execute 
          curNode.execute()
        })

        totalResults = totalResults + nodes.last.getResultCount() 
        batchesExecuted = batchesExecuted + 1
        println("Progress: Total results: %d, batchesExecuted: %d".format(totalResults, batchesExecuted))
      }
    })

    // print summary
    println("Total results: %d, batchesExecuted: %d".format(totalResults, batchesExecuted))
  }

  def print(): Unit = {
    nodes.foreach({ node =>
      println(node.toString) 
      println(Range(0, 150).map(x => "-").mkString(""))
    })
  }
}

object RunQuery {
  def main(args: Array[String]): Unit = {

    // initialize spark session
    val spark = SparkSession.builder().enableHiveSupport().appName("TEST").getOrCreate()
    import spark.implicits._
    
    // input output directories
    val inputDir = "data/input"
    val outputDir = "data/output"

    // initialize
    init(spark, inputDir)

    // initialize the pillars
    lazy val table1 = new Table1()
    lazy val hierarchy1 = new Hierarchy()
    lazy val hierarchy2 = new Hierarchy()
 
    lazy val nodes = Seq(
      table1.as("Table1")
      hierarchy1.as("Child1")
      hierarchy2.as("Child2").time_window(60).min_group_size(10)
    )
 
    // create the graph
    lazy val graphInput = QueryGraphInput(spark, workingBaseDir = outputDir, samplingDepth = 1, maxResults = 1000, batchKey = "id1", id1BasePrefix = "1", id2BasePrefix = "")
    lazy val graph = new QueryGraph(name = "TEST1", nodes = nodes, graphInput)

    graph.build()
    graph.print()
    graph.execute()
  }
  
  /**
   * Read all the tables that we need.
   */
  def init(spark: SparkSession, inputDir: String): Unit = {
    // TODO
  }
}
