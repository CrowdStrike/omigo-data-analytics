package omigo_core
import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.util.zip.GZIPInputStream
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import collection.JavaConverters._
// import scala.jdk.CollectionConverters

object FilePathsUtil {
  def read_filepaths(path: String, start_date_str: String, end_date_str: String, fileprefix: String, s3_region: String = null, aws_profile: String = null, granularity: String = "hourly",
    ignore_missing: Boolean = false) {
    throw new RuntimeException("Not implemented in Java")
  }

  def get_etl_level_prefix(curdate: String, etl_level: String) {
    throw new RuntimeException("Not implemented in Java")
  }

  def read_filepaths_hourly(path: String, start_date_str: String, end_date_str: String, fileprefix: String, s3_region: String = null, aws_profile: String = null, etl_level: String = "",
    ignore_missing: Boolean = false) {
    throw new RuntimeException("Not implemented in Java")
  }

  def check_exists(path: String, s3_region: String = null, aws_profile: String = null): Boolean = {
    if (path.startsWith("s3://") && S3Wrapper.check_path_exists(path, s3_region = s3_region, aws_profile = aws_profile))
      return true

    if ((new File(path)).exists())
      return true

    return false
  }

  def read_filepaths_daily(path: String, start_date_str: String, end_date_str: String, fileprefix: String, s3_region: String = null, aws_profile: String = null, etl_level: String = "",
    ignore_missing: Boolean = false) {
    throw new RuntimeException("Not implemented in Java")
  }

  def has_same_headers(filepaths: List[String], s3_region: String = null, aws_profile: String = null) {
    throw new RuntimeException("Not implemented in Java")
  }

  def create_header_map(header: String) {
    throw new RuntimeException("Not implemented in Java")
  }

  def create_header_index_map(header: String) {
    throw new RuntimeException("Not implemented in Java")
  }

  def read_file_content_as_lines(path: String, s3_region: String = null, aws_profile: String = null): List[String] = {
    var data: List[String] = null

    // check for s3
    if (path.startsWith("s3://")) {
      val (bucketName, objectKey) = Utils.split_s3_path(path)
      data = S3Wrapper.get_s3_file_content_as_text(bucketName, objectKey, s3_region = s3_region, aws_profile = aws_profile)
        .split("\n")
        .toList
    } else {
      if (path.endsWith(".gz")) {
        val inputStream = new GZIPInputStream(new FileInputStream(new File(path)))
        data = scala.io.Source.fromInputStream(inputStream).getLines().toList
        inputStream.close()
      } else if (path.endsWith(".zip")) {
        throw new RuntimeException("Not implemented")
      } else {
        data = scala.io.Source.fromFile(path).getLines().toList
      }
    }

    // simple csv parser
    if (path.endsWith(".csv") || path.endsWith("csv.gz") || path.endsWith(".csv.zip"))
        throw new RuntimeException("Not implemented")

    // return
    data
  }

  def create_date_numeric_representation(date_str: String, default_suffix: String) = {
    // check for yyyy-MM-dd
    if (date_str.length == 10) {
        date_str.replaceAll("-", "") + default_suffix
    } else {
      val instant = FuncLib.datestr_to_datetime(date_str)
      val local_date_time = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
      val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHMMss")
      local_date_time.format(formatter).toString()
    }
  }

  def get_file_paths_by_datetime_range(path: String, start_date_str: String, end_date_str: String, prefix: String, spillover_window: Int = 1, num_par: Int = 10,
    wait_sec: Int = 1, s3_region: String = null, aws_profile: String = null): List[String] = {
    // parse dates
    val start_date = FuncLib.datestr_to_datetime(start_date_str)
    val end_date = FuncLib.datestr_to_datetime(end_date_str)

    // get number of days inclusive start and end and include +/- 1 day buffer for overlap
    val num_days = Duration.between(start_date, end_date).toDays() + 1 + (spillover_window * 2)
    val start_date_minus_window = start_date.minus(Duration.ofDays(spillover_window))

    // create a numeric representation of date
    val start_date_num_str = create_date_numeric_representation(start_date_str, "000000")
    val end_date_num_str = create_date_numeric_representation(end_date_str, "999999")

    // create variable to store results
    val tasks = new scala.collection.mutable.ListBuffer[List[String]]() 

    // iterate and create tasks
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    Range(0, num_days.toInt).foreach({ d =>
      // generate the current path based on date
      val curDate = start_date_minus_window.plus(Duration.ofDays(d))
      val cur_path = path + "/dt=" + LocalDateTime.ofInstant(curDate, ZoneOffset.UTC).format(formatter)

      // get the list of files. This needs to be failsafe as not all directories may exist
      // TODO: add multi threading. TODO: potential bug in python
      // if (path.startsWith("s3://")):
      //   tasks.append(utils.ThreadPoolTask(s3_wrapper.get_directory_listing, cur_path, filter_func = None, fail_if_missing = False, region = s3_region, profile = aws_profile))
      // else:
      //   tasks.append(utils.ThreadPoolTask(get_local_directory_listing, cur_path, fail_if_missing = False))
      if (path.startsWith("s3://")) {
        tasks.append(S3Wrapper.get_directory_listing(cur_path, filter_func = null, ignore_if_missing = false, skip_exist_check = true, s3_region = s3_region, aws_profile = aws_profile))
      } else {
        tasks.append(get_local_directory_listing(cur_path, fail_if_missing = false, skip_exist_check = true))
      }
    })

    // execute the tasks
    // results = utils.run_with_thread_pool(tasks, num_par = num_par, wait_sec = wait_sec)
    val results = tasks.toList

    // final result
    val pathsFound = new scala.collection.mutable.ListBuffer[String]() 

    // iterate over results
    results.foreach({ filesList =>
      // debug. TODO: python code is broken. There is no curDate here
      // println("file_paths_util: get_file_paths_by_datetime_range: number of candidate files to read: curDate: %s, count: %d".format(curDate, filesList.length))
      println("file_paths_util: get_file_paths_by_datetime_range: number of candidate files to read: count: %d".format(filesList.length))

      // apply filter on the name and the timestamp
      filesList.foreach({ filename =>
        //format: full_prefix/fileprefix-startdate-enddate-starttime-endtime.tsv
        // get the last part after /
        //sep_index = filename.rindex("/")
        //filename1 = filename[sep_index + 1:]
        // TODO: python code is broken here
        val cur_path = filename.substring(0, filename.lastIndexOf("/")) 
        val base_filename = filename.substring(cur_path.length + 1)
        var ext_index: Int = -1
        // println("filename: " + filename) 

        // ignore any hidden files that start with dot(.)
        if (base_filename.startsWith(".")) {
          println("file_paths_util: get_file_paths_by_datetime_range: found hidden file. ignoring: %s".format(filename))
        } else {
          // get extension
          if (base_filename.endsWith(".tsv.gz"))
            ext_index = base_filename.lastIndexOf(".tsv.gz")
          else if (base_filename.endsWith(".tsv"))
            ext_index = base_filename.lastIndexOf(".tsv")
          else
            throw new RuntimeException("file_paths_util: get_file_paths_by_datetime_range: extension parsing failed: %s".format(filename))

          // proceed only if valid filename
          if (ext_index != -1) {
            // strip the extension
            val filename2 = base_filename.substring(0, ext_index)
            val filename3 = filename2.substring(prefix.length + 1)
            val parts = filename3.split("-").toList

            // the number of parts must be 3
            if (parts.length == 4) {
              // get the individual parts in the filename
              val curStartTs = "%s%s".format(parts(0), parts(1))
              val curEndTs = "%s%s".format(parts(2), parts(3))

              // apply the filter condition
              if (!(end_date_num_str < curStartTs || start_date_num_str > curEndTs)) {
                // note filename1
                pathsFound.append(filename)
                // println("file_paths_util: get_file_paths_by_datetime_range: found file: %s".format(filename))
              }
            }
          }
        }
      })
    })

    // return
    pathsFound.toList
  }

  def get_local_directory_listing(path: String, filter_func: (String) => Boolean = null, fail_if_missing: Boolean = true, skip_exist_check: Boolean = false): List[String] = {
    LocalFSWrapper.get_directory_listing(path, filter_func = filter_func, fail_if_missing = fail_if_missing, skip_exist_check = skip_exist_check)
  }

  def create_local_parent_dir(filepath: String) {
    throw new RuntimeException("Not implemented in Java")
  }

  def main(args: Array[String]): Unit = {
    // val path = "s3://tsv-data-analytics-sample/test-folder1/etl"
    // println("get_file_paths_by_date_time_range: " + get_file_paths_by_date_time_range(path, "2022-06-02", "2022-06-03", "data", 0, 0, 0, null, null))
  }
}

