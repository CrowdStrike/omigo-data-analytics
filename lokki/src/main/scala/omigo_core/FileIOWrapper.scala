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

class FileWriter(val output_file_name: String, val s3_region: String = null, val aws_profile: String = null) {
  // TODO: Not needed probably
}

class TSVFileWriter(val s3_region: String, val aws_profile: String) {
  def save(xtsv: TSV, output_file_name: String): Unit = {
    val content = List(xtsv.get_header(), xtsv.get_data().mkString("\n")).mkString("\n")

    // check s3 or local
    if (output_file_name.startsWith("s3://")) {
      val (bucket_name, object_key) = Utils.split_s3_path(output_file_name)
      S3Wrapper.put_s3_file_with_text_content(bucket_name, object_key, content, s3_region = s3_region, aws_profile = aws_profile)
      println("file saved to: " + output_file_name)
    } else {
      // get bytes
      var barr = content.getBytes()

      // check file extensions
      if (output_file_name.endsWith(".gz")) {
        val byteStream = new ByteArrayOutputStream(barr.length)
        val zipStream = new GZIPOutputStream(byteStream)
        zipStream.write(barr)
        zipStream.close()
        barr = byteStream.toByteArray()
      } else if (output_file_name.endsWith(".zip")) {
        throw new Exception("Not supported")
      }

      // open FileOutputStream and write
      val fos = new FileOutputStream(new File(output_file_name))
      fos.write(barr)
      fos.close()
      println("file saved to: " + output_file_name)
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
