package omigo_core
import java.time.LocalDateTime
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset

object FuncLib {
  def parse_image_file_base_name(x: Float) = {
    throw new Exception("Not Implemented for Java")
  }

  def uniq_len(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def uniq_mkstr(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def mean(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def mkstr(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def mkstr4f(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def minstr(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def maxstr(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def uniq_count(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def merge_uniq(vs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def select_first(x: Int, y: Int) = {
    throw new Exception("Not Implemented for Java")
  }

  def select_max_int(x: Int, y: Int) = {
    throw new Exception("Not Implemented for Java")
  }

  def str_arr_to_float(xs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def quantile(xs: List[Float], start: Int, end: Int, by: Float, precision: Int) = {
    throw new Exception("Not Implemented for Java")
  }

  def quantile4(xs: List[Float]) = {
    throw new Exception("Not Implemented for Java")
  }

  def quantile10(xs: List[Float]) = {
    throw new Exception("Not Implemented for Java")
  }

  def quantile40(xs: List[Float]) = {
    throw new Exception("Not Implemented for Java")
  }

  def max_str(xs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def min_str(xs: List[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def to2digit(x: Float) = {
    throw new Exception("Not Implemented for Java")
  }

  def to4digit(x: Float) = {
    throw new Exception("Not Implemented for Java")
  }

  def to6digit(x: Float) = {
    throw new Exception("Not Implemented for Java")
  }

  def convert_prob_to_binary(x: Float, split: Float) = {
    throw new Exception("Not Implemented for Java")
  }

  def get_str_map_with_keys(mp: Map[String, String], keys: Seq[String], fail_on_missing: Boolean) = {
    throw new Exception("Not Implemented for Java")
  }

  def get_str_map_without_keys(mp: Map[String, String], excluded_keys: Seq[String]) = {
    throw new Exception("Not Implemented for Java")
  }

  def dateTimeToUTCTimestamp(x: String) = {
    // 2022-05-20T05:00:00+00:00
    if (x.endsWith("UTC") || x.endsWith("GMT")) {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MMM-dd'T'HH:mm:ss z")
      val localDate = LocalDateTime.parse(x, formatter)
      localDate.toEpochSeconds(ZoneOffset.UTC) 
    } else if (x.endsWith("Z")) {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MMM-dd'T'HH:mm:ss'Z'")
      val localDate = LocalDateTime.parse(x, formatter)
      localDate.toEpochSeconds(ZoneOffset.UTC) 
    } else if (x.endsWith("+00:00")) {
      val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME 
      val localDate = LocalDateTime.parse(x, formatter)
      localDate.toEpochSeconds(ZoneOffset.UTC) 
    } else if (len(x) == 10 && x.find("-") != -1) {
      // 2021-11-01
      x = x + "T00:00:00Z"
      val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME 
      val localDate = LocalDateTime.parse(x, formatter)
      localDate.toEpochSeconds(ZoneOffset.UTC) 
    } else if (len(x) == 19) {
      // 2021-11-01T00:00:00
      x = x + "Z"
      val formatter = DateTimeFormatter.ofPattern("yyyy-MMM-dd'T'HH:mm:ss'Z'")
      val localDate = LocalDateTime.parse(x, formatter)
      localDate.toEpochSeconds(ZoneOffset.UTC) 
    } else if (len(x) == 26) {
      // 2021-11-01T00:00:00.000000
      x = x + "Z"
      val formatter = DateTimeFormatter.ofPattern("yyyy-MMM-dd'T'HH:mm:ss.S'Z'")
      val localDate = LocalDateTime.parse(x, formatter)
      localDate.toEpochSeconds(ZoneOffset.UTC) 
    } else if (x.length == 10 && x.forall(Character.isDigit) == true) {
      x.toLong
    } else if (x.length == 13 && x.forall(Character.isDigit) == true) {
      // this looks like numeric timestamp in millis
      return (x.toLong / 1000.0).toLong
    } else {
      throw new Exception("Unknown date format. Problem with UTC: " + x)
    }
  }

  def utcTimestampToDateTimeStr(x: Long) = {
    Instant.ofEpochSecond(x).toString()
  }

  def utcTimestamp_to_datetime(x: Long) = {
    throw new Exception("Not Implemented for Java")
  }

  def datetime_to_timestamp(x: String) = {
    throw new Exception("Not Implemented for Java")
  }

  def getUTCTimestampSec() = {
    (Instant.now().toEpochMilli() / 1000.0).toLong
  }

  def dateStrToDateTime(x: String) = {
    val utcTimestamp = dateTimeToUTCTimestamp(x)
    Instant.ofEpochSecond(utcTimestamp)
  }
}
