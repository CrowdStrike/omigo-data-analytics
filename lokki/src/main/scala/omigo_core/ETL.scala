package omigo_core
import scala.util.Random

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

  def scanByDateTimeRange(path: String, startDateStr: String, endDateStr: String, prefix: String, filterTransformFunc: Any, transformFunc: Any, spilloverWindow: Int, numPar: Int,
    waitSec: Int, timeoutSeconds: Int, defValMap: Map[String, String], samplingRate: Float, s3Region: String, awsProfile: String) = {

    // TODO: java and python differences
    if (filterTransformFunc != null || transformFunc != null)
      throw new Exception("function parameters are not implemented")

    // debug
    Utils.info("scan_by_datetime_range: path: %s, start_date_str: %s, end_date_str: %s, spillover_window: %s, def_val_map: %s, sampling_rate: %s".format(
        path, startDateStr, endDateStr, spilloverWindow, defValMap, samplingRate))

    // read filePaths by scanning. this involves listing all the files, and then matching the condititions
    var filePaths = getFilePathsByDateTimeRange(path, startDateStr, endDateStr, prefix, spilloverWindow, samplingRate, numPar, waitSec, s3Region, awsProfile)

    // debug
    Utils.info("scan_by_datetime_range: number of paths to read: %s, num_par: %d, timeout_seconds: %s".format(filePaths.length, numPar, timeoutSeconds))

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
        tsvList.append(TSVUtils.readWithFilterTransform(List(filePath), filterTransformFunc, transformFunc, s3Region, awsProfile))
    })

    // execute and get results
    // tsv_list = Utils.run_with_thread_pool(tasks, num_par = num_par, wait_sec = wait_sec)

    // combine all together
    val tsvCombined = TSV.merge(tsvList.toList, defValMap)
    Utils.info("scan_by_datetime_range: Number of records: %s".format(tsvCombined.num_rows()))

    // return
    tsvCombined 
  }

  def getFilePathsByDateTimeRange(path: String, startDateStr: String, endDateStr: String, prefix: String, spilloverWindow: Int, samplingRate: Float, numPar: Int, waitSec: Int, s3Region: String, awsProfile: String) = {
    // get all the filePaths
    var filePaths = FilePathsUtil.getFilePathsByDateTimeRange(path, startDateStr, endDateStr, prefix, spilloverWindow, numPar, waitSec, s3Region, awsProfile)

    // check for sampling rate
    if (samplingRate != -1) {
      // validation
      if (samplingRate < 0 || samplingRate > 1)
          throw new Exception("sampling_rate is not valid: %f".format(samplingRate))

      // determine the number of samples to take
      val sampleN = (filePaths.length * samplingRate).toInt
      filePaths = Random.shuffle(filePaths)
      filePaths = filePaths.take(sampleN).sorted
    } else {
      filePaths = filePaths.sorted
    }

    // return
    filePaths
  }

  def main(args: Array[String]): Unit = {
    val xtsv = scanByDateTimeRange(args(0), args(1), args(2), args(3), null, null, args(4).toInt, 0, 0, 0, Map.empty, 1.0f, null, null)
    xtsv.show()
  }
}

