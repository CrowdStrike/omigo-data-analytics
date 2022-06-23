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
  def read_filepaths(path: String, startDateStr: String, endDateStr: String, fileprefix: String, s3_region: String, aws_profile: String, granularity: String, ignore_missing: Boolean ) {
    throw new Exception("Not implemented in Java")
  }

  def get_etl_level_prefix(curdate: String, etl_level: String) {
    throw new Exception("Not implemented in Java")
  }

  def read_filepaths_hourly(path: String, startDateStr: String, endDateStr: String, fileprefix: String, s3_region: String, aws_profile: String, etl_level: String, ignore_missing: Boolean) {
    throw new Exception("Not implemented in Java")
  }

  def check_exists(path: String, s3_region: String, aws_profile: String) {
    throw new Exception("Not implemented in Java")
  }

  def read_filepaths_daily(path: String, startDateStr: String, endDateStr: String, fileprefix: String, s3_region: String, aws_profile: String, etl_level: String, ignore_missing: Boolean) {
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

  def createDateNumericRepresentation(dateStr: String, defaultSuffix: String) = {
    // check for yyyy-MM-dd
    if (dateStr.length == 10) {
        dateStr.replaceAll("-", "") + defaultSuffix
    } else {
      val instant = FuncLib.dateStrToDateTime(dateStr)
      val localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
      val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHMMss")
      localDateTime.format(formatter).toString()
    }
  }

  def getFilePathsByDateTimeRange(path: String, startDateStr: String, endDateStr: String, prefix: String, spilloverWindow: Int, numPar: Int, waitSec: Int, s3Region: String, awsProfile: String): List[String] = {
    // parse dates
    val startDate = FuncLib.dateStrToDateTime(startDateStr)
    val endDate = FuncLib.dateStrToDateTime(endDateStr)

    // get number of days inclusive start and end and include +/- 1 day buffer for overlap
    val numDays = Duration.between(startDate, endDate).toDays() + 1 + (spilloverWindow * 2)
    val startDateMinusWindow = startDate.minus(Duration.ofDays(spilloverWindow))

    // create a numeric representation of date
    val startDateNumStr = createDateNumericRepresentation(startDateStr, "000000")
    val endDateNumStr = createDateNumericRepresentation(endDateStr, "999999")

    // create variable to store results
    val tasks = new scala.collection.mutable.ListBuffer[List[String]]() 

    // iterate and create tasks
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    Range(0, numDays.toInt).foreach({ d =>
      // generate the current path based on date
      val curDate = startDateMinusWindow.plus(Duration.ofDays(d))
      val curPath = path + "/dt=" + LocalDateTime.ofInstant(curDate, ZoneOffset.UTC).format(formatter)

      // get the list of files. This needs to be failsafe as not all directories may exist
      // TODO: add multi threading. TODO: potential bug in python
      // if (path.startsWith("s3://")):
      //   tasks.append(utils.ThreadPoolTask(s3_wrapper.get_directory_listing, curPath, filter_func = None, fail_if_missing = False, region = s3_region, profile = aws_profile))
      // else:
      //   tasks.append(utils.ThreadPoolTask(get_local_directory_listing, curPath, fail_if_missing = False))
      if (path.startsWith("s3://")) {
        tasks.append(S3Wrapper.getDirectoryListing(curPath, null, false, null, null))
      } else {
        tasks.append(getLocalDirectoryListing(curPath, false))
      }
    })

    // execute the tasks
    // results = utils.run_with_thread_pool(tasks, numPar = numPar, waitSec = waitSec)
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
        val curPath = filename.substring(0, filename.lastIndexOf("/")) 
        val baseFilename = filename.substring(curPath.length + 1)
        var extIndex: Int = -1
        // println("filename: " + filename) 

        // ignore any hidden files that start with dot(.)
        if (baseFilename.startsWith(".")) {
          println("file_paths_util: get_file_paths_by_datetime_range: found hidden file. ignoring: %s".format(filename))
        } else {
          // get extension
          if (baseFilename.endsWith(".tsv.gz"))
            extIndex = baseFilename.lastIndexOf(".tsv.gz")
          else if (baseFilename.endsWith(".tsv"))
            extIndex = baseFilename.lastIndexOf(".tsv")
          else
            throw new Exception("file_paths_util: get_file_paths_by_datetime_range: extension parsing failed: %s".format(filename))

          // proceed only if valid filename
          if (extIndex != -1) {
            // strip the extension
            val filename2 = baseFilename.substring(0, extIndex)
            val filename3 = filename2.substring(prefix.length + 1)
            val parts = filename3.split("-").toList

            // the number of parts must be 3
            if (parts.length == 4) {
              // get the individual parts in the filename
              val curStartTs = "%s%s".format(parts(0), parts(1))
              val curEndTs = "%s%s".format(parts(2), parts(3))

              // apply the filter condition
              if (!(endDateNumStr < curStartTs || startDateNumStr > curEndTs)) {
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

  def getLocalDirectoryListing(path: String, fail_if_missing: Boolean): List[String] = {
    // TODO: python implementation is tricky and not taking relative paths properly. enforcing absolute paths here
    if (path.startsWith("/") == false) {
      throw new Exception("absolute path needed")
    }
    Files.list(new File(path).toPath()).iterator().asScala.toList.map(_.toString())
  }

  def create_local_parent_dir(filepath: String) {
    throw new Exception("Not implemented in Java")
  }

  def main(args: Array[String]): Unit = {
    val path = "s3://tsv-data-analytics-sample/test-folder1/etl"
    println("getFilePathsByDateTimeRange: " + getFilePathsByDateTimeRange(path, "2022-06-02", "2022-06-03", "data", 0, 0, 0, null, null))
  }
}

