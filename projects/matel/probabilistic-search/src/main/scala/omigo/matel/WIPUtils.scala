package omigo.matel

import collection.JavaConverters._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.File
import java.net.URI
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs._
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder,Encoders}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf, SparkFiles}
import HashUtils._
import org.apache.spark.broadcast.Broadcast
import java.util.BitSet
import org.apache.spark.api.java.function.FilterFunction
import scala.reflect.ClassTag

object WIPUtils {
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
}
