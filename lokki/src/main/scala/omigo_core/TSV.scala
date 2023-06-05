package omigo_core

class TSV(val header: String, val data: List[String]) {
  val header_fields = header.split("\t").filter({ h => h != "" }).toList
  val header_map = header_fields.zipWithIndex.toMap

  def get_header(): String = {
    header
  }

  def get_data(): List[String] = {
    data
  }

  def get_header_fields(): List[String] = {
    header_fields
  }

  def get_columns(): List[String] = {
    get_header_fields()
  }

  def get_header_map(): Map[String, Int] = {
    header_map
  }

  def num_rows(): Int = {
    if (data == null) 0 else data.length
  }

  def num_cols(): Int = {
     header_fields.length
  }

  def has_empty_header(): Boolean = {
    num_cols() == 0
  }

  def union(that_arr: List[TSV]): TSV = {
    // TODO: python has mixed types
    // check if this is a single element TSV or an array
    // if (type(tsv_or_that_arr) == TSV):
    //     that_arr = [tsv_or_that_arr]
    // else:
    //     that_arr = tsv_or_that_arr

    // boundary condition
    if (that_arr.length == 0)
      return this

    // check empty
    if (has_empty_header()) {
      // if there are multiple tsvs
      if (that_arr.length > 1)
        return that_arr(0).union(that_arr.drop(1))
      else
        return that_arr(0)
    }

    // validation
    that_arr.foreach({ that =>
      if (get_header() != that.get_header())
        throw new Exception("Headers are not matching for union: %s, %s".format(header_fields, header_fields))
    })

    // create new data
    val new_data = new scala.collection.mutable.ListBuffer[String]() 
    get_data().foreach({ line =>
        val fields = line.split("\t")
        if (fields.length != header_fields.length)
          throw new Exception("Invalid input data. Fields size are not same as header: header: %s, fields: %s".format(header_fields, fields))
        new_data.append(line)
    })

    that_arr.foreach({ that =>
      that.get_data().foreach({ line =>
        val fields = line.split("\t")
        if (fields.length != header_fields.length)
          throw new Exception("Invalid input data. Fields size are not same as header: header: %s, fields: %s".format(header_fields, fields))
        new_data.append(line)
      })
    })

    new TSV(header, new_data.toList)
  }

  def add_const(col: String, value: String, inherit_message: String): TSV = {
    // check empty
    if (has_empty_header()) {
      // checking empty value
      if (value == "") {
        Utils.warn("add_const: empty header and empty data. extending just the header")
        return TSV.new_with_cols(List(col), List.empty)
      } else {
        throw new Exception("add_const: empty header but non empty data: %s".format(value))
      }
    }

    // return
    val inherit_message2 = if (inherit_message.length > 0) inherit_message + ": add_const" else "add_const"
    // transform(List(header_fields(0)), (x: String) => value, col, inherit_message2)
    // TODO: java implementation doesnt use transform for minimal code
    val header2 = List(header, col).mkString("\t")
    val data2 = get_data().map({ line => List(line, value).mkString("\t") })
    new TSV(header2, data2)
  }

  def add_const_if_missing(col: String, value: String, inherit_message: String): TSV = {
    // check empty
    if (has_empty_header()) {
      // checking empty value
      if (value == "") {
        Utils.warn("add_const_if_missing: empty tsv and empty value. extending just the header")
        return TSV.new_with_cols(List(col), List.empty)
      } else {
        throw new Exception("add_const_if_missing: empty tsv but non empty value")
      }
    }

    // check for presence
    if (header_fields.contains(col)) {
      return this 
    } else {
      val inherit_message2 = if (inherit_message.length > 0) inherit_message + ": add_const_if_missing" else "add_const_if_missing"
      return add_const(col, value, inherit_message2)
    }
  }

  def select(col_or_cols: Any, inherit_message: String): TSV = {
    // check empty
    if (has_empty_header())
      throw new Exception("select: empty tsv")

    // get matching column and indexes
    val matching_cols = __get_matching_cols__(col_or_cols, false)
    val indexes = __get_col_indexes__(matching_cols)

    // create new header
    val new_header = matching_cols.mkString("\t")

    // create new data
    var counter = 0
    val new_data = new scala.collection.mutable.ListBuffer[String]()
    get_data().foreach({ line =>
      // report progress
      counter = counter + 1
      Utils.report_progress("select: [1/1] selecting columns", inherit_message, counter, data.length)

      val fields = line.split("\t")
      val new_fields = new scala.collection.mutable.ListBuffer[String]()
      indexes.foreach({ i =>
        if (i >= fields.length)
          throw new Exception("Invalid index")
          // col_or_cols, matching_cols, indexes, line, fields, len(fields), len(header_fields), header_map)
        new_fields.append(fields(i))
      })
      new_data.append(new_fields.mkString("\t"))
    })

    // return
    new TSV(new_header, new_data.toList)
  }

  def to_tuples5(col_or_cols: Any, inherit_message: String): Seq[(String, String, String, String, String)] = {
    // check empty
    if (has_empty_header())
      throw new Exception("to_tuples5: empty tsv")

    // get matching column and indexes
    val matching_cols = __get_matching_cols__(col_or_cols, false)
    val indexes = __get_col_indexes__(matching_cols)

    var counter = 0
    val new_data = new scala.collection.mutable.ListBuffer[(String, String, String, String, String)]()
    get_data().foreach({ line =>
      // report progress
      counter = counter + 1
      Utils.report_progress("to_tuples5: [1/1] selecting columns", inherit_message, counter, data.length)

      val fields = line.split("\t")
      new_data.append((fields(indexes(0)), fields(indexes(1)), fields(indexes(2)), fields(indexes(3)), fields(indexes(4))))
    })

    new_data.toSeq
  }

  // TODO: java implementation supports partially
  def __get_matching_cols__(col_or_cols_org: Any, ignore_if_missing: Boolean): List[String] = {
    def __java_re_match__(pstr: String, v: String) = {
      pstr.r.findAllIn(v).size > 0
    }

    var col_or_cols = if (col_or_cols_org.isInstanceOf[String]) List(col_or_cols_org.asInstanceOf[String]) else col_or_cols_org.asInstanceOf[List[String]] 

    // handle boundary conditions
    if (col_or_cols == null || col_or_cols.length == 0)
        return List.empty 

    // check if there is comma. If yes, then map it to array
    if (col_or_cols.exists(_.contains(",")))
        col_or_cols = col_or_cols.flatMap({ t => t.split(",") }) 

    // check if this is a single col name or an array
    // val is_array = Utils.is_array_of_string_values(col_or_cols)
    val is_array = true

    // create name.
    // col_patterns = []
    // if (is_array == True):
    //     col_patterns = col_or_cols
    // else:
    //     col_patterns.append(col_or_cols)
    val col_patterns = col_or_cols

    // now iterate through all the column names, check if it is a regular expression and find
    // all matching ones
    val matching_cols = new scala.collection.mutable.ListBuffer[String]() 
    col_patterns.foreach({ col_pattern =>
      // check for matching columns for the pattern
      var col_pattern_found = false

      // iterate through header
      get_header_fields().foreach({ h =>
        // check for match
        if ((col_pattern.indexOf(".*") != -1 && col_pattern.r.findAllIn(h).size > 0) || (col_pattern == h)) {
          col_pattern_found = true
          // if not in existing list then add
          if (matching_cols.contains(h) == false)
            matching_cols.append(h)
        }
      })

      // throw new exception if some col or pattern is not found
      if (col_pattern_found == false) {
        Utils.raise_exception_or_warn("Col name or pattern not found: %s, %s".format(col_pattern, header_fields), ignore_if_missing)
        return List.empty
      }
    })

    // return
    return matching_cols.toList
  }

  def __has_matching_cols__(col_or_cols: Any, ignore_if_missing: Boolean): Boolean = {
    try {
      if (__get_matching_cols__(col_or_cols, ignore_if_missing).length > 0)
        true
      else
        false
    } catch {
        case _: Exception => false
    }
  }

  def __get_col_indexes__(cols: List[String]): List[Int] = {
    val indexes = new scala.collection.mutable.ListBuffer[Int]() 
    cols.foreach({ c =>
      indexes.append(header_map(c))
    })

    // return
    return indexes.toList
  }

  // TODO: this is temp implementation
  def show(): TSV = {
    println("Stats: num_cols: %s, num_rows: %s, header: %s".format(num_cols(), num_rows(), header_fields))
    println(get_header())
    get_data().foreach({ line => println(line) })
    return this
  }
}

object TSV {
  def get_version() {
    throw new Exception("Not Implemented")
  }

  def get_func_name(f: Any) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_init(func_name: String) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_update(arr: Any, v: Any, func_name: Any) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_update_sum(arr: Any, v: Any) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_update_mean(arr: Any, v: Any) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_update_min(arr: Any, v: Any) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_update_max(arr: Any, v: Any) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_update_len(arr: Any, v: Any) {
    throw new Exception("Not Implemented")
  }

  def get_rolling_func_closing(arr: Any, func_name: Any) {
    throw new Exception("Not Implemented")
  }

  // TODO: python code needs fixes for profile
  def read(path_or_paths: Any, sep: String): TSV = {
    TSVUtils.read(path_or_paths, sep, null, null)
  }

  def write(xtsv: TSV, path: String) {
    throw new Exception("Not Implemented")
  }

  def merge(xtsvs: List[TSV], def_val_map: Map[String, String]): TSV = {
    // warn if def_val_map is not defined
    if (def_val_map == null)
      Utils.warn("merge: use merge_union or merge_intersect")

    // return
    TSVUtils.merge(xtsvs, def_val_map)
  }

  def merge_union(xtsvs: List[TSV], def_val_map: Map[String, String]): TSV = {
    // check def_val_map
    if (def_val_map == null)
      throw new Exception("merge_union: def_val_map can not be none for union. Use merge_intersect instead")

    // return
    TSVUtils.merge(xtsvs, def_val_map)
  }

  def merge_intersect(xtsvs: List[TSV]): TSV = {
    TSVUtils.merge(xtsvs, null)
  }

  def exists(path: String): Boolean = {
    TSVUtils.check_exists(path, null, null)
  }

  def from_df(df: Any) {
    throw new Exception("Not Implemented")
  }

  def from_maps(mps: List[Map[String, String]]) {
    throw new Exception("Not Implemented")
  }

  def enable_debug_mode() {
    throw new Exception("Not Implemented")
  }

  def disable_debug_mode() {
    throw new Exception("Not Implemented")
  }

  def set_report_progress_perc(perc: Float) {
    throw new Exception("Not Implemented")
  }

  def set_report_progress_min_thresh(thresh: Int) {
    throw new Exception("Not Implemented")
  }

  def newWithCols(cols: List[String], data: List[String]): TSV = {
    new_with_cols(cols, data)
  }

  def new_with_cols(cols: List[String], data: List[String]): TSV = {
    new TSV(cols.mkString("\t"), data) 
  }

  def create_empty(): TSV = {
    new_with_cols(List.empty, List.empty) 
  }

  def main(args: Array[String]): Unit = {
    TSV.read(args.toList, null).show()      
  }
}
