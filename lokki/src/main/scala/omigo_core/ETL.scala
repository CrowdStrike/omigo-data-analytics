package omigo_core
import scala.util.Random
import collection.JavaConverters._

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

  def scan_by_datetime_range(path: String, start_date_str: String, end_date_str: String, prefix: String, filter_transform_func: Any = null, transform_func: Any = null,
    spillover_window: Int = 1, num_par: Int = 5, wait_sec: Int = 5, timeout_seconds: Int = 600, def_val_map: Map[String, String] = Map.empty, sampling_rate: Float = -999f,
    s3_region: String = null, aws_profile: String = null) = {

    // TODO: java and python differences
    if (filter_transform_func != null || transform_func != null)
      throw new Exception("function parameters are not implemented")

    // debug
    Utils.info("scan_by_datetime_range: path: %s, start_date_str: %s, end_date_str: %s, spillover_window: %s, def_val_map: %s, sampling_rate: %s".format(
        path, start_date_str, end_date_str, spillover_window, def_val_map, sampling_rate))

    // read filePaths by scanning. this involves listing all the files, and then matching the condititions
    var filePaths = get_file_paths_by_datetime_range(path, start_date_str, end_date_str, prefix, spillover_window, sampling_rate, num_par, wait_sec, s3_region, aws_profile)

    // debug
    Utils.info("scan_by_datetime_range: number of paths to read: %s, num_par: %d, timeout_seconds: %s".format(filePaths.length, num_par, timeout_seconds))

    // do some checks on the headers in the filePaths

    // debug
    Utils.debug("tsvUtils: scan_by_datetime_range: number of files to read: %d".format(filePaths.length))

    // read all the files in the filePath applying the filter function
    // val tasks = null
    val tsvList = new scala.collection.mutable.ListBuffer[TSV]() 

    // iterate over filePaths and submit
    filePaths.foreach({ filePath =>
        // TODO: multithreading
        // tasks.append(Utils.ThreadPoolTask(tsvUtils.read_with_filter_transform, filePath, filter_transform_func, transform_func, s3_region, aws_profile))
        tsvList.append(TSVUtils.read_with_filter_transform(List(filePath), filter_transform_func = filter_transform_func, transform_func = transform_func,
          s3_region = s3_region, aws_profile = aws_profile))
    })

    // execute and get results
    // tsv_list = Utils.run_with_thread_pool(tasks, num_par = num_par, wait_sec = wait_sec)

    // combine all together
    val tsvCombined = TSV.merge(tsvList.toList, def_val_map)
    Utils.info("scan_by_datetime_range: Number of records: %s".format(tsvCombined.num_rows()))

    // return
    tsvCombined 
  }

  def get_file_paths_by_datetime_range(path: String, start_date_str: String, end_date_str: String, prefix: String, spillover_window: Int = 1, sampling_rate: Float = -999f,
    num_par: Int = 10, wait_sec: Int = 1, s3_region: String = null, aws_profile: String = null) = {
    // get all the filePaths
    var filePaths = FilePathsUtil.get_file_paths_by_datetime_range(path, start_date_str, end_date_str, prefix, spillover_window = spillover_window, num_par = num_par,
      wait_sec = wait_sec, s3_region = s3_region, aws_profile = aws_profile)

    // check for sampling rate
    if (sampling_rate != -1) {
      // validation
      if (sampling_rate < 0 || sampling_rate > 1)
          throw new Exception("sampling_rate is not valid: %f".format(sampling_rate))

      // determine the number of samples to take
      val sampleN = (filePaths.length * sampling_rate).toInt
      filePaths = Random.shuffle(filePaths)
      filePaths = filePaths.take(sampleN).sorted
    } else {
      filePaths = filePaths.sorted
    }

    // return
    filePaths
  }

  // def main(args: Array[String]): Unit = {
  //   val xtsv = scan_by_datetime_range(args(0), args(1), args(2), args(3), null, null, args(4).toInt, 0, 0, 0, Map.empty, 1.0f, null, null)
  //   xtsv.show()
  // }
}

