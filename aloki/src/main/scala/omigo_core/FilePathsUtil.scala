package omigo_core
import java.io.File
import java.io.FileInputStream
import java.util.zip.GZIPInputStream

object FilePathsUtil {
  def read_filepaths(path: String, start_date_str: String, end_date_str: String, fileprefix: String, s3_region: String, aws_profile: String, granularity: String, ignore_missing: Boolean ) {
    throw new Exception("Not implemented in Java")
  }

  def get_etl_level_prefix(curdate: String, etl_level: String) {
    throw new Exception("Not implemented in Java")
  }

  def read_filepaths_hourly(path: String, start_date_str: String, end_date_str: String, fileprefix: String, s3_region: String, aws_profile: String, etl_level: String, ignore_missing: Boolean) {
    throw new Exception("Not implemented in Java")
  }

  def check_exists(path: String, s3_region: String, aws_profile: String) {
    throw new Exception("Not implemented in Java")
  }

  def read_filepaths_daily(path: String, start_date_str: String, end_date_str: String, fileprefix: String, s3_region: String, aws_profile: String, etl_level: String, ignore_missing: Boolean) {
    throw new Exception("Not implemented in Java")
  }

  def has_same_headers(filepaths: List[String], s3_region: String, aws_profile: String) {
    throw new Exception("Not implemented in Java")
  }

  def create_header_map(header: String) {
    throw new Exception("Not implemented in Java")
  }

  def create_header_index_map(header: String) {
    throw new Exception("Not implemented in Java")
  }

  def readFileContentAsLines(path: String, s3Region: String, awsProfile: String): List[String] = {
    var data: List[String] = null

    // check for s3
    if (path.startsWith("s3://")) {
      val (bucketName, objectKey) = Utils.splitS3Path(path)
      data = S3Wrapper.getS3FileContentAsText(bucketName, objectKey, s3Region, awsProfile)
        .split("\n")
        .toList
    } else {
      if (path.endsWith(".gz")) {
        val inputStream = new GZIPInputStream(new FileInputStream(new File(path)))
        data = scala.io.Source.fromInputStream(inputStream).getLines().toList
        inputStream.close()
      } else if (path.endsWith(".zip")) {
        throw new Exception("Not implemented")
      } else {
        data = scala.io.Source.fromFile(path).getLines().toList
      }
    }

    // simple csv parser
    if (path.endsWith(".csv") || path.endsWith("csv.gz") || path.endsWith(".csv.zip"))
        throw new Exception("Not implemented")

    // return
    data
  }

  def create_date_numeric_representation(date_str: String, default_suffix: String) {
    throw new Exception("Not implemented in Java")
  }

  def get_file_paths_by_datetime_range(path: String, start_date_str: String, end_date_str: String, prefix: String, spillover_window: Int, num_par: Int, wait_sec: Int, s3_region: String, aws_profile: String) {
    throw new Exception("Not implemented in Java")
  }

  def get_local_directory_listing(path: String, fail_if_missing: Boolean) {
    throw new Exception("Not implemented in Java")
  }

  def create_local_parent_dir(filepath: String) {
    throw new Exception("Not implemented in Java")
  }
}

