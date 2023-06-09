package omigo_core
import collection.JavaConverters._
import java.io._
import java.nio.ByteBuffer
import java.util.Random
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

object LocalFSWrapper {
  def check_path_exists(path: String) = {
    new File(path).exists()
  }

  def check_file_exists(path: String) = {
    new File(path).exists()
  }

  def get_directory_listing(path: String, filter_func: (String) => Boolean = null, fail_if_missing: Boolean = true, skip_exist_check: Boolean = false): List[String] = {
    if (skip_exist_check == false) {
      if (check_path_exists(path) == false) {
        if (fail_if_missing == true)
          throw new RuntimeException("Directory does not exist: %s".format(path))
      }
    }

    val full_paths = (new File(path).listFiles()).map({ p => path + "/" + p }).toList
    
    // apply filter func if any
    if (filter_func != null)
      full_paths.filter({ p => filter_func(p) })
    else
      full_paths
  } 

  def makedirs(path: String, exist_ok: Boolean = true) = {
    if (check_path_exists(path) == false) {
      new File(path).mkdir()
    } else {
      if (exist_ok == true)
        true
      else
        throw new RuntimeException("makedirs: path already exists: %s, exist_ok is false".format(path))
    }
  }

  def get_file_content_as_text(path: String) = {
    // check for file exists
    if (check_path_exists(path) == false)
        throw new RuntimeException("file doesnt exist: %s".format(path))

    // create data
    var data: String = null

    // read based on file type
    if (path.endsWith(".gz")) {
      val inputStream = new GZIPInputStream(new FileInputStream(new File(path)))
      data = scala.io.Source.fromInputStream(inputStream).getLines().mkString("\n")
      inputStream.close()
    } else if (path.endsWith(".zip")) {
      throw new Exception("Not implemented")
    } else {
      data = scala.io.Source.fromFile(path).getLines().mkString("\n")
    }

    data
  }

  def get_last_modified_timestamp(path: String) = {
    throw new RuntimeException("Not implemented for java")
  }

  def put_file_with_text_content(path: String, text: String) = {
    val printWriter = new PrintWriter(path)
    printWriter.write(text)
    printWriter.close() 
  }

  def delete_file(path: String, fail_if_missing: Boolean = false): Unit = {
    // check if the file exists
    if (check_path_exists(path) == false) {
      if (fail_if_missing)
        throw new RuntimeException("delete_dir: path doesnt exist: %s".format(path))
      else
        Utils.debug("delete_dir: path doesnt exist: %s".format(path))

      return
    }

    // delete
    new File(path).delete()
  }

  def delete_dir(path: String, fail_if_missing: Boolean = false): Unit = {
    // check if the file exists
    if (check_path_exists(path) == false) {
      if (fail_if_missing)
        throw new RuntimeException("delete_dir: path doesnt exist: %s".format(path))
      else
        Utils.debug("delete_dir: path doesnt exist: %s".format(path))

      return
    }

    // delete
    new File(path).delete()
  }
}
