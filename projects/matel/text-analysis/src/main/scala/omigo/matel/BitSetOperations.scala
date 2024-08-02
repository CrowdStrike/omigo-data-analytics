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

/**
 * FilterFunction implementation for bitset filters.
 */
class BitSetFilterFunctions(filterBitSetAndMapBC: Broadcast[Map[String, BitSet]], filterBitSetOrMapBC: Broadcast[Map[String, Seq[BitSet]]]) extends FilterFunction[Row] {

  // Filter Definition
  override def call(row: Row): Boolean = {
    // check for and clause
    val filter1 = filterBitSetAndMapBC.value.forall({ case (name, filterBitSet) =>
      val dataBitSet = deserializeBitSet(row.get(row.fieldIndex(name)).asInstanceOf[Array[Byte]])
      val bitSet = new BitSet()
      bitSet.or(filterBitSet)
      bitSet.and(dataBitSet)
      bitSet.cardinality() == filterBitSet.cardinality()
    })

    // early return if no match in and clause
    if (filter1 == false) {
      return false
    }

    // check for or clause
    val filter2 = filterBitSetOrMapBC.value.forall({ case (name, filterBitSets) =>
      val dataBitSet = deserializeBitSet(row.get(row.fieldIndex(name)).asInstanceOf[Array[Byte]])
      filterBitSets.exists({ filterBitSet =>
        // set the bits from first condition, then xor to clear them. Check if bitset is 
        val bitSet = new BitSet()
        bitSet.or(filterBitSet)
        bitSet.and(dataBitSet)
        bitSet.cardinality() == filterBitSet.cardinality()
      })
    })

    filter2
  }

  // clear the broadcast variables.
  def unpersist(): Unit = {
    filterBitSetAndMapBC.unpersist()
    filterBitSetOrMapBC.unpersist()
  }
}

/**
 * create filter class for working with data frame.
 */
class InputFileNameBatchFilter(val numBatchesBC: Broadcast[Int], val batchNumberBC: Broadcast[Int]) extends FilterFunction[Row] {
  override def call(row: Row): Boolean = {
    val inputFileName = row.getString(row.fieldIndex("input_file_name"))
    inputFileName.hashCode().abs % numBatchesBC.value == batchNumberBC.value
  }

  def unpersist(): Unit = {
    numBatchesBC.unpersist()
    batchNumberBC.unpersist()
  }
}

class BatchInput(val df: DataFrame) {
  var count = -1L

  def getDataFrame(): DataFrame = {
    df
  }

  def getCount(): Long = {
    // cache the count
    if (count < 0)
      if (df == null)
        count = 0L
      else
        count = df.count

    count
  }
}

/**
 * Class for doing map and reduce operations using bit set filters.
 */
class BitSetOperations {
  // reducebykey method for map bitset
  def bitSetFilterReduceByKey(cardinalityThreshold: Float, bitSetsFilter1: Seq[BitSet], bitSetsText1: Seq[BitSet], bitSetsId1_1: Seq[BitSet],
    bitSetsId2_1: Seq[BitSet],counts1: Seq[Int],
    bitSetsFilter2: Seq[BitSet], bitSetsText2: Seq[BitSet], bitSetsId1_2: Seq[BitSet], bitSetsId2_2: Seq[BitSet], counts2: Seq[Int]) = {
    
    val bitSetTextCombined = new BitSet()
    bitSetTextCombined.or(bitSetsText1.head)
    bitSetTextCombined.or(bitSetsText2.head)
    
    // check for exceeding cardinality. both for command line args and aid
    if ((1f * bitSetTextCombined.cardinality / bitSetTextCombined.size) > cardinalityThreshold) {
      // reset
      if (bitSetsText1.head.cardinality < bitSetsText2.head.cardinality)
        (bitSetsFilter1 ++ bitSetsFilter2, bitSetsText1 ++ bitSetsText2, 
        bitSetsId1_1 ++ bitSetsId1_2, bitSetsId2_1 ++ bitSetsId2_2, counts1 ++ counts2)
      else
        (bitSetsFilter2 ++ bitSetsFilter1, bitSetsText2 ++ bitSetsText1,
        bitSetsId1_2 ++ bitSetsId1_1, bitSetsId2_2 ++ bitSetsId2_1, counts2 ++ counts1)
    } else {
      val bitSetFilterCombined = new BitSet()
      bitSetFilterCombined.or(bitSetsFilter1.head)
      bitSetFilterCombined.or(bitSetsFilter2.head)
      
      val bitSetId1Combined = new BitSet()
      bitSetId1Combined.or(bitSetsId1_1.head)
      bitSetId1Combined.or(bitSetsId1_2.head)
      
      val bitSetId2Combined = new BitSet()
      bitSetId2Combined.or(bitSetsId2_1.head)
      bitSetId2Combined.or(bitSetsId2_2.head)
      
      (
        bitSetFilterCombined +: (bitSetsFilter1.drop(1) ++ bitSetsFilter2.drop(1)),
        bitSetTextCombined +: (bitSetsText1.drop(1) ++ bitSetsText2.drop(1)),
        bitSetId1Combined +: (bitSetsId1_1.drop(1) ++ bitSetsId1_2.drop(1)),
        bitSetId2Combined +: (bitSetsId2_1.drop(1) ++ bitSetsId2_2.drop(1)),
        (counts1.head + counts2.head) +: (counts1.drop(1) ++ counts2.drop(1))
      )
    }
  }

  // bitset mapping for all keys. FIXME: Partitioning strategy
  def bitSetMapFunction(spark: SparkSession, inputDir: String, outputDir: String) = {
    // Some thresholds
    val BitSetCardinalityThreshold = 0.15f
    val BitSetCardinalityThresholdId1 = 0.3f
    val BitSetCardinalityThresholdId2 = 100

    // events with bitset filters merged together
    val events = spark.read.parquet(inputDir).filter("custom_filter is not null").withColumn("input_file_name", input_file_name()).rdd.map({ row =>
      val id1 = row.getString(row.fieldIndex("id1"))
      val id2 = row.getString(row.fieldIndex("id2"))
      val ts = row.getLong(row.fieldIndex("ts"))
      val customFilter = row.get(row.fieldIndex("custom_filter")).asInstanceOf[Array[Byte]]
      val customFilterText = row.get(row.fieldIndex("custom_filter_text")).asInstanceOf[Array[Byte]]
      val inputFileName = row.getString(row.fieldIndex("input_file_name"))

      val customFilterId1 = new BitSet()
      val customFilterId2 = constructWordsHash(Seq(id2))

      ((inputFileName, id1), (Seq(deserializeBitSet(customFilter)), Seq(deserializeBitSet(customFilterText)), Seq(customFilterId1), Seq(customFilterId2), Seq(1)))
    })
    .reduceByKey({ case ((bitSetsFilter1, bitSetsText1, bitSetsId1_1, bitSetsId2_1, counts1), (bitSetsFilter2, bitSetsText2, bitSetsId1_2, bitSetsId2_2, counts2)) =>
      bitSetFilterReduceByKey(BitSetCardinalityThreshold, bitSetsFilter1, bitSetsText1, bitSetsId1_1, bitSetsId2_1, counts1,
        bitSetsFilter2, bitSetsText2, bitSetsId1_2, bitSetsId2_2, counts2)
    })
    .flatMap({ case ((inputFileName, id1), (bitSetsFilter, bitSetsText, bitSetsId1, bitSetsId2, counts)) =>
      val bitSetsTuple = bitSetsFilter.zip(bitSetsText).zip(bitSetsId1).zip(bitSetsId2).zip(counts)

      // dropping id1
      bitSetsTuple.map({ case ((((bitSetFilter, bitSetText), bitSetsId1), bitSetsId2), count) =>
        (inputFileName, (Seq(bitSetFilter), Seq(bitSetText), Seq(bitSetsId1), Seq(bitSetsId2), Seq(count)))
      })
    })
    .reduceByKey({ case ((bitSetsFilter1, bitSetsText1, bitSetsId1_1, bitSetsId2_1, counts1),
      (bitSetsFilter2, bitSetsText2, bitSetsId1_2, bitSetsId2_2, counts2)) =>

      bitSetFilterReduceByKey(BitSetCardinalityThresholdId1, bitSetsFilter1, bitSetsText1, bitSetsId1_1,
        bitSetsId2_1, counts1, bitSetsFilter2, bitSetsText2, bitSetsId1_2, bitSetsId2_2, counts2)
    })
    .flatMap({ case (inputFileName, (bitSetsFilter, bitSetsText, bitSetsId1, bitSetsId2, counts)) =>
      val bitSetsTuple = bitSetsFilter.zip(bitSetsText).zip(bitSetsId1).zip(bitSetsId2).zip(counts)

      // generate output
      bitSetsTuple.map({ case ((((bitSetFilter, bitSetText), bitSetsId1), bitSetsId2), count) =>
        Row.fromSeq(Seq(
          serializeBitSetToByteArray(bitSetFilter),
          serializeBitSetToByteArray(bitSetText),
          serializeBitSetToByteArray(bitSetsId1),
          serializeBitSetToByteArray(bitSetsId2),
          inputFileName, count, bitSetFilter.cardinality, bitSetText.cardinality, bitSetsTuple.size, bitSetsId2.cardinality
        ))
      })
    })

    // define schema as getting no TypeTag error in sbt. TODO
    val schema = StructType(Array(
      StructField("custom_filter", BinaryType, true),
      StructField("custom_filter_text", BinaryType, true),
      StructField("custom_filter_id1", BinaryType, true),
      StructField("custom_filter_id2", BinaryType, true),
      StructField("input_file_name", StringType, true),
      StructField("custom_filter_text_num_rows", LongType, true),
      StructField("custom_filter_cardinality", LongType, true),
      StructField("custom_filter_text_cardinality", LongType, true),
      StructField("custom_filter_text_parent_list_count", LongType, true),
      StructField("custom_filter_id2_cardinality", LongType, true),
    ))

    // create dataframe
    val df = spark.createDataFrame(events, schema)

    // persist
    df.write.mode(SaveMode.Overwrite).parquet(outputDir)
  }
}
