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

