package omigo_core
// import scala.jdk.CollectionConverters
import collection.JavaConverters._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.util.Random
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class FileWriter(val outputFileName: String, val s3Region: String, val awsProfile: String) {
  // TODO: Not needed probably
}

class TSVFileWriter(val s3Region: String, val awsProfile: String) {
  def save(xtsv: TSV, outputFileName: String): Unit = {
    val content = List(xtsv.getHeader(), xtsv.getData().mkString("\n")).mkString("\n")

    // check s3 or local
    if (outputFileName.startsWith("s3://")) {
      val (bucketName, objectKey) = Utils.splitS3Path(outputFileName)
      S3Wrapper.putS3FileWithTextContent(bucketName, objectKey, content, s3Region, awsProfile)
      println("file saved to: " + outputFileName)
    } else {
      // get bytes
      var barr = content.getBytes()

      // check file extensions
      if (outputFileName.endsWith(".gz")) {
        val byteStream = new ByteArrayOutputStream(barr.length)
        val zipStream = new GZIPOutputStream(byteStream)
        zipStream.write(barr)
        zipStream.close()
        barr = byteStream.toByteArray()
      } else if (outputFileName.endsWith(".zip")) {
        throw new Exception("Not supported")
      }

      // open FileOutputStream and write
      val fos = new FileOutputStream(new File(outputFileName))
      fos.write(barr)
      fos.close()
      println("file saved to: " + outputFileName)
    }
  }
}

class FileReader {
  // TODO: Not needed
}

object TSVFileWriter {
  def main(args: Array[String]): Unit = {
    val xtsv = new TSV("col1", List("value1", "value2")) 
    val writer = new TSVFileWriter(null, null)
    writer.save(xtsv, args(0))
  }
}
