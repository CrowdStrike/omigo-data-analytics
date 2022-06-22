package omigo_core

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

  def readFileContentAsLines(path: String, s3_region: String, aws_profile: String): List[String] = {
    throw new Exception("Not implemented in Java")
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

