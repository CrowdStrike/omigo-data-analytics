package omigo_core

object TSVUtils {
  def merge(tsv_list: List[TSV], def_val_map: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

  def split_headers_in_common_and_diff(tsv_list: List[TSV]) {
    throw new Exception("Not Implemented")
  }

  def get_diffs_in_headers(tsv_list: List[TSV]) {
    throw new Exception("Not Implemented")
  }

  def merge_intersect(tsv_list: List[TSV], def_val_map: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

  def read(input_file_or_files: List[String], sep: String, s3_region: String, aws_profile: String) {
    throw new Exception("Not Implemented")
  }

  def read_with_filter_transform(input_file_or_files: List[String], filter_transform_func: Any, transform_func: Any, s3_region: String, aws_profile: String) {
    throw new Exception("Not Implemented")
  }

  def read_by_date_range(path: String, start_date_str: String, end_date_str: String, prefix: String, s3_region: String, aws_profile: String, granularity: String) {
    throw new Exception("Not Implemented")
  }

  def load_from_dir(path: String, start_date_str: String, end_date_str: String, prefix: String, s3_region: String, aws_profile: String, granularity: String) {
    throw new Exception("Not Implemented")
  }

  def load_from_files(filepaths: List[String], s3_region: String, aws_profile: String) {
    throw new Exception("Not Implemented")
  }

  def load_from_array_of_map(map_arr: List[Map[String, String]]) {
    throw new Exception("Not Implemented")
  }

  def save_to_file(xtsv: TSV, output_file_name: String, s3_region: String, aws_profile: String) {
    throw new Exception("Not Implemented")
  }

  def check_exists(xtsv: TSV, s3_region: String, aws_profile: String) {
    throw new Exception("Not Implemented")
  }

  def sort_func(vs: Int) {
    throw new Exception("Not Implemented")
  }

  def __read_base_url__(url: String, query_params : Map[String, String], headers : Map[String, String], body: String, username: String, password: String, timeout_sec: Int, verify: Boolean) {
    throw new Exception("Not Implemented")
  }

  def read_url_json(url: String, query_params : Map[String, String], headers : Map[String, String], body: String, username: String, password: String, timeout_sec: Int, verify: Boolean) {
    throw new Exception("Not Implemented")
  }

  def read_url_response(url: String, query_params : Map[String, String], headers : Map[String, String], body: String, username: String, password: String, timeout_sec: Int, verify: Boolean, num_retries: Int, retry_sleep_sec: Int) {
    throw new Exception("Not Implemented")
  }

  def read_url(url: String, query_params : Map[String, String], headers : Map[String, String], sep: String, username: String, password: String, timeout_sec: Int, verify: Boolean) {
    throw new Exception("Not Implemented")
  }

  def read_url_as_tsv(url: String, query_params : Map[String, String], headers : Map[String, String], sep: String, username: String, password: String, timeout_sec: Int, verify: Boolean) {
    throw new Exception("Not Implemented")
  }

  def from_df(df: Any) {
    throw new Exception("Not Implemented")
  }

  def __get_argument_as_array__(arg_or_args: List[String]) {
    throw new Exception("Not Implemented")
  }

  def scan_by_datetime_range() {
    throw new Exception("Not Implemented")
  }

  def get_file_paths_by_datetime_range() {
    throw new Exception("Not Implemented")
  }

}
