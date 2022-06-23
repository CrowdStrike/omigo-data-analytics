package omigo_core

object Utils {
  def is_critical() {
    throw new Exception("Not Implemented")
  }

  def is_error() {
    throw new Exception("Not Implemented")
  }

  def is_warn() {
    throw new Exception("Not Implemented")
  }

  def is_info() {
    throw new Exception("Not Implemented")
  }

  def is_debug() {
    throw new Exception("Not Implemented")
  }

  def is_trace() {
    throw new Exception("Not Implemented")
  }

  def get_report_progress() {
    throw new Exception("Not Implemented")
  }

  def get_report_progress_min_thresh() {
    throw new Exception("Not Implemented")
  }

  def set_report_progress_perc(perc: Float) {
    throw new Exception("Not Implemented")
  }

  def set_report_progress_min_thresh(thresh: Int) {
    throw new Exception("Not Implemented")
  }

  def trace(msg: String) {
    throw new Exception("Not Implemented")
  }

  def trace_once(msg: String, msg_cache: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

  def debug(msg: String) {
    throw new Exception("Not Implemented")
  }

  def debug_once(msg: String, msg_cache: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

  def info(msg: String) {
    throw new Exception("Not Implemented")
  }

  def info_once(msg: String, msg_cache: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

  def error(msg: String) {
    throw new Exception("Not Implemented")
  }

  def error_once(msg: String, msg_cache: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

  def enable_critical_mode() {
    throw new Exception("Not Implemented")
  }

  def enable_error_mode() {
    throw new Exception("Not Implemented")
  }

  def enable_warn_mode() {
    throw new Exception("Not Implemented")
  }

  def enable_info_mode() {
    throw new Exception("Not Implemented")
  }

  def enable_debug_mode() {
    throw new Exception("Not Implemented")
  }

  def enable_trace_mode() {
    throw new Exception("Not Implemented")
  }

  def disable_critical_mode() {
    throw new Exception("Not Implemented")
  }

  def disable_error_mode() {
    throw new Exception("Not Implemented")
  }

  def disable_warn_mode() {
    throw new Exception("Not Implemented")
  }

  def disable_info_mode() {
    throw new Exception("Not Implemented")
  }

  def disable_debug_mode() {
    throw new Exception("Not Implemented")
  }

  def disable_trace_mode() {
    throw new Exception("Not Implemented")
  }

  def warn(msg: String) {
    throw new Exception("Not Implemented")
  }

  def warn_once(msg: String, msg_cache: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

  def is_code_todo_warning() {
    throw new Exception("Not Implemented")
  }

  def print_code_todo_warning(msg: String) {
    throw new Exception("Not Implemented")
  }

  def url_encode(s: String) {
    throw new Exception("Not Implemented")
  }

  def url_decode(s: String) {
    throw new Exception("Not Implemented")
  }

  def url_decode_clean(s: String) {
    throw new Exception("Not Implemented")
  }

  def parse_encoded_json(s: String) {
    throw new Exception("Not Implemented")
  }

  def encode_json_obj(json_obj: Any) {
    throw new Exception("Not Implemented")
  }

  // len(s3://) = 5
  def splitS3Path(path: String): (String, String) = {
    val part1 = path.substring(5)
    val index = part1.indexOf("/")
    val bucketName = part1.substring(0, index)

    // boundary conditions
    val objectKey = if (index < part1.length() - 1) part1.substring(index + 1) else ""

    // remove trailing suffix
    val objectKey2 = if (objectKey.endsWith("/")) objectKey.substring(0, objectKey.length() - 1) else objectKey 

    // return
    return (bucketName, objectKey2)
  }

  def get_counts_map(xs: List[String]) {
    throw new Exception("Not Implemented")
  }

  def report_progress(msg: String, inherit_message: String, counter: Int, total: Int) {
    throw new Exception("Not Implemented")
  }

  def merge_arrays(arr_list: List[Array[String]]) {
    throw new Exception("Not Implemented")
  }

  def is_array_of_string_values(col_or_cols: String) {
    throw new Exception("Not Implemented")
  }

  def is_numeric(v: String) {
    throw new Exception("Not Implemented")
  }

  def is_float(v: String) {
    throw new Exception("Not Implemented")
  }

  def is_int_col(xtsv: TSV, col: String) {
    throw new Exception("Not Implemented")
  }

  def is_float_col(xtsv: TSV, col: String) {
    throw new Exception("Not Implemented")
  }

  def is_pure_float_col(xtsv: TSV, col: String) {
    throw new Exception("Not Implemented")
  }

  def is_float_with_fraction(xtsv: TSV, col: String) {
    throw new Exception("Not Implemented")
  }

  def compute_hash(x: String, seed: Int) {
    throw new Exception("Not Implemented")
  }


  class ThreadPoolTask {
    //   def __init__(self, func, *args, **kwargs) {
    throw new Exception("Not Implemented")
  }

  def run_with_thread_pool(tasks: Any, num_par: Int, wait_sec: Int, post_wait_sec: Int) {
    throw new Exception("Not Implemented")
  }

  def raise_exception_or_warn(msg: String, ignore_if_missing: Boolean) {
    throw new Exception("Not Implemented")
  }

  def strip_spl_white_spaces(v: String) {
    throw new Exception("Not Implemented")
  }

  def resolve_meta_params(xstr: String, props: Map[String, String]) {
    throw new Exception("Not Implemented")
  }

}

