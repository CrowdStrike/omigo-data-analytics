package omigo_core

class EtlDateTimePathFormat {
  throw new Exception("Not implemented")
}

object ETL {
  def get_matching_etl_date_time_path(path: String, new_base_path: String, new_prefix: String, new_extension: String) {
    throw new Exception("Not Implemented")
  }

  def get_etl_date_str_from_ts(ts: Long) {
    throw new Exception("Not Implemented")
  }

  def get_etl_datetime_str_from_ts(ts: Long) {
    throw new Exception("Not Implemented")
  }

  def get_etl_file_date_str_from_ts(ts: Long) {
    throw new Exception("Not Implemented")
  }

  def get_etl_file_datetime_str_from_ts(ts: Long) {
    throw new Exception("Not Implemented")
  }

  def get_etl_file_base_name_by_ts(prefix: String, start_ts: Long, end_ts: Long) {
    throw new Exception("Not Implemented")
  }

  def scan_by_datetime_range(path: String, start_date_str: String, end_date_str: String, prefix: String, filter_transform_func: Any, transform_func: Any, spillover_window: Int, num_par: Int,
    wait_sec: Int, timeout_seconds: Int, def_val_map: Map[String, String], sampling_rate: Float, s3_region: String, aws_profile: String) {
    throw new Exception("Not Implemented")
  }

  def get_file_paths_by_datetime_range(path: String, start_date_str: String, end_date_str: String, prefix: String, spillover_window: Int, sampling_rate: Float, num_par: Int, wait_sec: Int, s3_region: String, aws_profile: String) {
    throw new Exception("Not Implemented")
  }
}

