package omigo.matel

import collection.JavaConverters._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.BitSet
import scala.util.Random

object HashUtils {

  val NumBits = 1
  val HashBitsSpace = 30
  val MaxStrLen = 100
  val HashSpaceRatio = 5

  val RegexPattern = "[\\\\/ ,\\*\\%:;\\(\\)\\{\\}\\+\\-\\=\\|<>\\&\\?\\[\\]\\$\\n\\@]"
  val MaxWordLen = 30

  // FIXME
  def computeHash(phrase: String, hashSpace: Int, numBits: Int) = {
    val seed = phrase.hashCode.abs
    val rand = new Random(seed)

    // find numBits within a spaec of k
    val values = (for {
      i <- Range(0, numBits)
    } yield {
      rand.nextInt().abs % hashSpace
    })

    values.toSet
  }

  def computeHashFirstBit(phrase: String, hashSpace: Int = 1024) = {
    val seed = phrase.hashCode.abs
    val rand = new Random(seed)

    rand.nextInt().abs % hashSpace
  }

  def constructSearchableHashStr(str: String, hashSpace: Int, phraseLength: Int): String = {
    if (hashSpace == 0 || phraseLength == 0) {
      ""
    } else {
      val bitSet = constructSearchableHashBitSet(str, hashSpace, phraseLength)
      getBitsString(bitSet)
    }
  }

  def constructSearchableHash(str: String, hashSpace: Int, phraseLength: Int): Array[Byte] = {
    val bitSet = constructSearchableHashBitSet(str, hashSpace, phraseLength)

    serializeBitSetToByteArray(bitSet)
  }

  def getBitsString(bitSet: BitSet): String = {
    var i = 0
    var str = ""
    var sep = ""
    var nextBitPos = bitSet.nextSetBit(i)
    while (nextBitPos >= 0) {
      str = str + sep + nextBitPos
      sep = ","
      nextBitPos = bitSet.nextSetBit(nextBitPos + 1)
    }
    str
  }

  def deserializeBitSet(byteArray: Array[Byte]): BitSet = {
    // val bis = new ByteArrayInputStream(byteArray)
    // val in = new ObjectInputStream(bis)
    // in.readObject().asInstanceOf[BitSet]
    BitSet.valueOf(byteArray)
  }
  
  def serializeBitSetToByteArray(bitSet: BitSet): Array[Byte] = {
    // val baos = new ByteArrayOutputStream()
    // val out = new ObjectOutputStream(baos)
    // out.writeObject(bitSet)
    // out.flush()

    // baos.toByteArray()
    bitSet.toByteArray()
  }

  def constructSearchableHashBitSet(str: String, hashSpace: Int, phraseLength: Int) = {
    val bitSet = new BitSet(hashSpace)

    generatePhrases(str, phraseLength).foreach({ x =>
      computeHash(x, hashSpace, 1).foreach({ bitPos => bitSet.set(bitPos) })
    })

    bitSet
  }

  def generateSubStringPhrases(word: String, phraseLength: Int) = {
    if (word.length <= phraseLength)
      Seq(word)
    else
      Seq(word.substring(0, phraseLength), word.substring(word.length - phraseLength))
  }

  def generatePhrases(str: String, phraseLength: Int) = {
    if (str == null) {
      Seq.empty
    } else {
      (for {
        word <- str.split("[:\\\\/,; -]").filter(_.trim.length > 0)
        phrase <- generateSubStringPhrases(word, phraseLength)
      } yield {
        // println("Phrase: " + phrase)
        phrase
      }).toSeq
    }
  }

  def generatePhrasesV1(str: String, phraseLength: Int) = {
    if (str == null) {
      Seq.empty
    } else {
      (for {
        word <- str.split("[:\\\\/,; -]").filter(_.trim.length > 0)
        if (word.length <= MaxStrLen);
        i <- Range(0, word.length - (phraseLength - 1))
        phrase = word.substring(i, i + phraseLength)
      } yield {
        // println("Phrase: " + phrase)
        phrase
      }).toSeq
    }
  }

  def constructWordsHash(words: Seq[String], hashBitsSpace: Int = 1024) = {
    val bitSet = new BitSet(hashBitsSpace)
    words.foreach({ word =>
      computeHash(word, hashBitsSpace, 1).foreach({ bitPos => bitSet.set(bitPos) })
    })

    bitSet
  }

  def getHashSpace(str: String) = {
    if (str == null || str.length == 0) {
      0
    } else {
      val ratioSpace =  math.ceil(str.length / HashSpaceRatio).toInt
      if (ratioSpace < HashBitsSpace) HashBitsSpace else ratioSpace
    }
  }
}
