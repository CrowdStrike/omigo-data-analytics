package omigo.matel

import collection.JavaConverters._
import java.util.BitSet
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

/**
 * FilterFunction implementation for bitset filters.
 */
class BitSetFilterFunctions(filterBitSetAndMapBC: Broadcast[Map[String, BitSet]], filterBitSetOrMapBC: Broadcast[Map[String, Seq[BitSet]]]) extends FilterFunction[Row] {
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

  /**
   * clear the broadcast variables.
   */
  def unpersist(): Unit = {
    filterBitSetAndMapBC.unpersist()
    filterBitSetOrMapBC.unpersist()
  }
}

class BitSetFunctions {
  // reducebykey method for map bitset
  def bitsetFilterReduceByKey(cardinalityThreshold: Float, bitSetsPName1: Seq[BitSet], bitSetsImagePath1: Seq[BitSet], bitSetsCmdArgs1: Seq[BitSet],
    bitSetsCmdArgsPrefix3_1: Seq[BitSet],
    bitSetsCid1: Seq[BitSet], bitSetsAid1: Seq[BitSet], bitSetsPid1: Seq[BitSet], bitSetsUid1: Seq[BitSet], counts1: Seq[Int],
    bitSetsPName2: Seq[BitSet], bitSetsImagePath2: Seq[BitSet], bitSetsCmdArgs2: Seq[BitSet], bitSetsCmdArgsPrefix3_2: Seq[BitSet],
    bitSetsCid2: Seq[BitSet], bitSetsAid2: Seq[BitSet], bitSetsPid2: Seq[BitSet], bitSetsUid2: Seq[BitSet], counts2: Seq[Int]) = {
    
    val bitSetCmdArgsCombined = new BitSet()
    bitSetCmdArgsCombined.or(bitSetsCmdArgs1.head)
    bitSetCmdArgsCombined.or(bitSetsCmdArgs2.head)
    
    // check for exceeding cardinality. both for command line args and aid
    if ((1f * bitSetCmdArgsCombined.cardinality / bitSetCmdArgsCombined.size) > cardinalityThreshold) {
      // reset
      if (bitSetsCmdArgs1.head.cardinality < bitSetsCmdArgs2.head.cardinality)
        (bitSetsPName1 ++ bitSetsPName2, bitSetsImagePath1 ++ bitSetsImagePath2, bitSetsCmdArgs1 ++ bitSetsCmdArgs2, bitSetsCmdArgsPrefix3_1 ++ bitSetsCmdArgsPrefix3_2,
        bitSetsCid1 ++ bitSetsCid2, bitSetsAid1 ++ bitSetsAid2, bitSetsPid1 ++ bitSetsPid2, bitSetsUid1 ++ bitSetsUid2, counts1 ++ counts2)
      else
        (bitSetsPName2 ++ bitSetsPName1, bitSetsImagePath2 ++ bitSetsImagePath1, bitSetsCmdArgs2 ++ bitSetsCmdArgs1, bitSetsCmdArgsPrefix3_2 ++ bitSetsCmdArgsPrefix3_1,
        bitSetsCid2 ++ bitSetsCid1, bitSetsAid2 ++ bitSetsAid1, bitSetsPid2 ++ bitSetsPid1, bitSetsUid2 ++ bitSetsUid1, counts2 ++ counts1)
    } else {
      val bitSetPNameCombined = new BitSet()
      bitSetPNameCombined.or(bitSetsPName1.head)
      bitSetPNameCombined.or(bitSetsPName2.head)
      
      val bitSetImagePathCombined = new BitSet()
      bitSetImagePathCombined.or(bitSetsImagePath1.head)
      bitSetImagePathCombined.or(bitSetsImagePath2.head)
      
      val bitSetCmdArgsPrefix3Combined = new BitSet()
      bitSetCmdArgsPrefix3Combined.or(bitSetsCmdArgsPrefix3_1.head)
      bitSetCmdArgsPrefix3Combined.or(bitSetsCmdArgsPrefix3_2.head)
      
      val bitSetCidCombined = new BitSet()
      bitSetCidCombined.or(bitSetsCid1.head)
      bitSetCidCombined.or(bitSetsCid2.head)
      
      val bitSetAidCombined = new BitSet()
      bitSetAidCombined.or(bitSetsAid1.head)
      bitSetAidCombined.or(bitSetsAid2.head)
      
      val bitSetPidCombined = new BitSet()
      bitSetPidCombined.or(bitSetsPid1.head)
      bitSetPidCombined.or(bitSetsPid2.head)

      val bitSetUidCombined = new BitSet()
      bitSetUidCombined.or(bitSetsUid1.head)
      bitSetUidCombined.or(bitSetsUid2.head)

      (
        bitSetPNameCombined +: (bitSetsPName1.drop(1) ++ bitSetsPName2.drop(1)),
        bitSetImagePathCombined +: (bitSetsImagePath1.drop(1) ++ bitSetsImagePath2.drop(1)),
        bitSetCmdArgsCombined +: (bitSetsCmdArgs1.drop(1) ++ bitSetsCmdArgs2.drop(1)),
        bitSetCmdArgsPrefix3Combined +: (bitSetsCmdArgsPrefix3_1.drop(1) ++ bitSetsCmdArgsPrefix3_2.drop(1)),
        bitSetCidCombined +: (bitSetsCid1.drop(1) ++ bitSetsCid2.drop(1)),
        bitSetAidCombined +: (bitSetsAid1.drop(1) ++ bitSetsAid2.drop(1)),
        bitSetPidCombined +: (bitSetsPid1.drop(1) ++ bitSetsPid2.drop(1)),
        bitSetUidCombined +: (bitSetsUid1.drop(1) ++ bitSetsUid2.drop(1)),
        (counts1.head + counts2.head) +: (counts1.drop(1) ++ counts2.drop(1))
      )
    }
  }
}

