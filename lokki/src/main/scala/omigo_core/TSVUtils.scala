package omigo_core

object TSVUtils {
  def merge(tsv_list_org: List[TSV], def_val_map: Map[String, String]): TSV = {
    var tsv_list = tsv_list_org

    // validation
    if (tsv_list.length == 0) {
      Utils.warn("Error in input. List of tsv is empty")
      return TSV.create_empty()
    }

    // remove tsvs without any columns
    tsv_list = tsv_list.filter({ xtsv => xtsv.num_cols() > 0 })

    // base condition
    if (tsv_list.length == 0) {
      Utils.warn("List of tsv is empty. Returning")
      return TSV.create_empty()
    }

    // check for valid headers
    val header = tsv_list(0).get_header()
    val header_fields = tsv_list(0).get_header_fields()

    // iterate to check mismatch in header
    var index = 0
    tsv_list.foreach({ t => 
      // Use a different method for merging if the header is different
      if (header != t.get_header()) {
        val header_diffs = get_diffs_in_headers(tsv_list)
        if (header_diffs.length > 0) {
          // TODO
          if (def_val_map == null)
            Utils.warn("Mismatch in header at index: %s. Cant merge. Using merge_intersect for common intersection. Some of the differences in header: %s".format(index, header_diffs))
        } else {
          Utils.warn("Mismatch in order of header fields: %s, %s. Using merge intersect".format(header.split("\t"), t.get_header().split("\t")))
        }
        return merge_intersect(tsv_list, def_val_map)
      }

      index = index + 1
    })

    // simple condition
    if (tsv_list.length == 1)
      return tsv_list(0)
    else
      return tsv_list(0).union(tsv_list.drop(1))
  }

  def split_headers_in_common_and_diff(tsv_list: List[TSV]): (List[String], List[String]) = {
    val common = new scala.collection.mutable.HashMap[String, Int]() 
    // get the counts for each header field
    tsv_list.foreach({ t =>
      t.get_header_fields().foreach({ h =>
        if (common.contains(h) == false)
          common(h) = 0
        common(h) = common(h) + 1
      })
    })

    // find the columns which are not present everywhere
    val non_common = new scala.collection.mutable.ListBuffer[String]() 
    common.foreach({ case (k, v) =>
      if (v != tsv_list.length)
        non_common.append(k)
    })

    // return
    (common.keys.toList.sorted, non_common.toList.sorted)
  }

  def get_diffs_in_headers(tsv_list: List[TSV]): List[String] = {
    val (common, non_common) = split_headers_in_common_and_diff(tsv_list)
    non_common
  }

  def merge_intersect(tsv_list_org: List[TSV], def_val_map: Map[String, String]): TSV = {
    var tsv_list = tsv_list_org

    // remove zero length tsvs
    tsv_list = tsv_list_org.filter({ xtsv => xtsv.num_cols() > 0 })

    // base condition
    if (tsv_list.length == 0)
      throw new Exception("List of tsv is empty")

    // boundary condition
    if (tsv_list.length == 1)
      return tsv_list(0)

    // get the first header
    val header_fields = tsv_list(0).get_header_fields()

    // some debugging
    val diff_cols = get_diffs_in_headers(tsv_list)
    val same_cols = new scala.collection.mutable.ListBuffer[String]() 
    header_fields.foreach({ h =>
      if (diff_cols.contains(h) == false)
        same_cols.append(h)
    })

    // print if number of unique headers are more than 1
    if (diff_cols.length > 0) {
      // debug
      Utils.debug("merge_intersect: missing columns: %s".format(diff_cols))

      // check which of the columns among the diff have default values
      if (def_val_map != null) {
        // create effective map with empty string as default value
        val effective_def_val_map = new scala.collection.mutable.HashMap[String, String]() 

        // some validation. the default value columns should exist somewhere
        def_val_map.keys.foreach({ h =>
          // check if all default columns exist
          if (diff_cols.contains(h) == false && same_cols.contains(h) == false)
            throw new Exception("Default value for a column given which does not exist: %s".format(h))
        })

        // assign empty string to the columns for which default value was not defined
        diff_cols.foreach({ h =>
          if (def_val_map.contains(h)) {
            Utils.debug("merge_intersect: assigning default value for %s: %s".format(h, def_val_map(h)))
            effective_def_val_map(h) = def_val_map(h)
          } else {
            Utils.debug("merge_intersect: assigning empty string as default value to column: %s".format(h))
            effective_def_val_map(h) = ""
          }
        })

        // get the list of keys in order
        val keys_order = new scala.collection.mutable.ListBuffer[String]()
        header_fields.foreach({ h =>
          keys_order.append(h)
        })

        // append the missing columns
        diff_cols.foreach({ h =>
          if (header_fields.contains(h) == false)
            keys_order.append(h)
        })

        println("diff_cols: %s, keys_order: %s".format(diff_cols, keys_order)) 

        // create a list of new tsvs
        val new_tsvs = new scala.collection.mutable.ListBuffer[TSV]() 
        tsv_list.foreach({ t =>
          var t1 = t
          diff_cols.foreach({ d =>
            t1 = t1.add_const_if_missing(d, effective_def_val_map(d), "")
          })
          new_tsvs.append(t1.select(keys_order.toList, ""))
        })

        // return after merging
        return merge(new_tsvs.toList, Map.empty)
      } else {
        // handle boundary condition of no matching cols
        if (same_cols.length == 0) {
          return TSV.create_empty()
        } else {
          // create a list of new tsvs
          val new_tsvs = new scala.collection.mutable.ListBuffer[TSV]() 
          tsv_list.foreach({ t =>
            new_tsvs.append(t.select(same_cols.toList, ""))
          })

          return merge(new_tsvs.toList, null)
        }
      }
    } else {
      // probably landed here because of mismatch in headers position
      val tsv_list2 = new scala.collection.mutable.ListBuffer[TSV]() 
      tsv_list.foreach({ t =>
        tsv_list2.append(t.select(same_cols.toList, ""))
      })
      return merge(tsv_list2.toList, null)
    }
  }

  // TODO : this is minimal implementation. python code needs to fix aws settings
  def read(input_file_or_files: Any, sep: String, s3_region: String, aws_profile: String): TSV = {
    val input_files = if (input_file_or_files.isInstanceOf[String]) List(input_file_or_files.asInstanceOf[String]) else input_file_or_files.asInstanceOf[List[String]]

    // input_files = __get_argument_as_array__(input_file_or_files)
    val tsv_list = new scala.collection.mutable.ListBuffer[TSV]() 
    input_files.foreach({ input_file =>
      // check if it is a file or url
      if (input_file.startsWith("http")) {
        // tsv_list.append(read_url_as_tsv(input_file))
        throw new Exception("http format not supported")
      } else {
        // read file content
        val lines = FilePathsUtil.readFileContentAsLines(input_file, s3_region, aws_profile)

        // take header and dat
        val header = lines(0)
        val data = lines.drop(1) 

        // check if a custom separator is defined
        if (sep != null)
          throw new Exception("custom sep is not supported")

        tsv_list.append(new TSV(header, data))
      }
    })

    return merge(tsv_list.toList, null)
  }

  def readWithFilterTransform(inputFileOrFiles: List[String], filterTransformFunc: Any, transformFunc: Any, s3Region: String, awsProfile: String): TSV = {
    throw new Exception("TBD")
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

  // TODO: python needs fixing
  def check_exists(path: String, s3_region: String, aws_profile: String): Boolean = {
    FilePathsUtil.check_exists(path, s3_region, aws_profile)
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

  def main(args: Array[String]): Unit = {

  }
}
