package omigo_core

class S3IOWrapper {
  val RESERVED_HIDDEN_FILE = ".omigo.ignore"
  val WAIT_SEC = 1

  val DEFAULT_WAIT_SEC = 3
  val DEFAULT_ATTEMPTS = 3

  def __is_s3__(path: String) = {
    path.startsWith("s3://")
  }

  def file_exists(path: String): Boolean = {
    if (__is_s3__(path))
      __s3_file_exists__(path)
    else
      __local_file_exists__(path)
  }

  def __s3_file_exists__(path: String): Boolean = {
    val path2 = __normalize_path__(path)
    S3Wrapper.check_file_exists(path)
  }

  def __local_file_exists__(path: String): Boolean = {
    LocalFSWrapper.check_file_exists(path)
  }

  def file_exists_with_wait(path: String, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): Boolean = {
    // check if exists
    if (file_exists(path)) {
      true
    } else {
      // wait on false 
      if (attempts > 0) {
        Utils.info("exists_with_wait: path: %s, attempts: %s, waiting for %s seconds".format(path, attempts, wait_sec))
        Thread.sleep(wait_sec)
        file_exists_with_wait(path, wait_sec = wait_sec, attempts = attempts - 1)
      } else {
        false
      }
    }
  }

  def dir_exists(path: String): Boolean = {
    val dir_file_path = "%s/%s".format(path, RESERVED_HIDDEN_FILE)
    file_exists(dir_file_path)
  }

  def dir_exists_with_wait(path: String, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): Boolean = {
    val dir_file_path = "%s/%s".format(path, RESERVED_HIDDEN_FILE)
    file_exists_with_wait(dir_file_path, wait_sec = wait_sec, attempts = attempts)
  }

  def file_not_exists(path: String): Boolean = {
    if (__is_s3__(path))
      __s3_file_not_exists__(path)
    else
      __local_file_not_exists__(path)
  }

  def __s3_file_not_exists__(path: String) = {
    val path2 = __normalize_path__(path)
    S3Wrapper.check_file_exists(path2) == false
  }

  def __local_file_not_exists__(path: String) = {
    val path2 = __normalize_path__(path)
    LocalFSWrapper.check_file_exists(path2) == false
  }

  def file_not_exists_with_wait(path: String, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): Boolean = {
    // check if exists
    if (file_not_exists(path)) {
        true
    } else {
      // wait on false 
      if (attempts > 0) {
        Utils.info("not_exists_with_wait: path: %s, attempts: %s, waiting for %s seconds".format(path, attempts, wait_sec))
        Thread.sleep(wait_sec)
        file_not_exists_with_wait(path, wait_sec = wait_sec, attempts = attempts - 1)
      } else {
        false
      }
    }
  }

  def dir_not_exists_with_wait(path: String, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): Boolean = {
    val dir_file_path = "%s/%s".format(path, RESERVED_HIDDEN_FILE)
    file_not_exists_with_wait(dir_file_path, wait_sec = wait_sec, attempts = attempts)
  }

  def read_file_contents_as_text_with_wait(path: String, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): String = {
    val path2 = __normalize_path__(path)

    // go into a wait loop for s3 to sync if the path is missing
    if (file_exists(path2) == false && attempts > 0) {
        Utils.info("read: path: %s doesnt exist. waiting for %s seconds. attempts: %s".format(path2, wait_sec, attempts))
        Thread.sleep(wait_sec)
        read_file_contents_as_text_with_wait(path2, wait_sec = wait_sec, attempts = attempts - 1)
    } else {
      // return
      read_file_contents_as_text(path)
    }
  }

  def read_file_contents_as_text(path: String): String = {
    if (__is_s3__(path))
      __s3_read_file_contents_as_text__(path)
    else
      __local_read_file_contents_as_text__(path)
  }

  def __s3_read_file_contents_as_text__(path: String): String = {
    val path2 = __normalize_path__(path)
    val (bucket_name, object_key) = Utils.split_s3_path(path2)
    S3Wrapper.get_file_content_as_text(bucket_name, object_key)
  }

  def __local_read_file_contents_as_text__(path: String): String = {
    val path2 = __normalize_path__(path)
    LocalFSWrapper.get_file_content_as_text(path)
  }

  def __normalize_path__(path: String): String = {
    val path2 = {
      if (path.endsWith("/"))
        path.take(path.length - 1)
      else
        path
    }
    
    if (path2.endsWith("/"))
        throw new RuntimeException("Multiple trailing '/' characters found: %s".format(path2))

    path2
  }

  def __simplify_dir_list__(path: String, listings: Seq[String], include_reserved_files: Boolean = false): Seq[String] = {
    val path2 = __normalize_path__(path)

    // get index to simplify output
    val index = path2.length
    val result1 = listings.filter({ t => include_reserved_files == true || t.endsWith("/" + RESERVED_HIDDEN_FILE) == false })
    val result2 = result1.map({ t => t.drop(index + 1) })

    // return
    result2
  }

  def ls(path: String, include_reserved_files: Boolean = false, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS, skip_exist_check: Boolean = false): Seq[String] = {
    val path2 = __normalize_path__(path)
    
    // go into a wait loop for s3 to sync if the path is missing
    if (dir_exists(path2) == false && attempts > 0) {
      Utils.info("ls: path: %s doesnt exist. waiting for %s seconds. attempts: %s".format(path2, wait_sec, attempts))
      Thread.sleep(wait_sec)
      ls(path2, include_reserved_files = include_reserved_files, wait_sec = wait_sec, attempts = attempts - 1)
    } else {
      // get directory listings 
      val listings = get_directory_listing(path2, skip_exist_check = skip_exist_check)
      __simplify_dir_list__(path2, listings, include_reserved_files = include_reserved_files)
    }
  }

  def get_directory_listing(path: String, skip_exist_check: Boolean = false): Seq[String] = {
    if (__is_s3__(path))
      __s3_get_directory_listing__(path, skip_exist_check = skip_exist_check)
    else
      __local_get_directory_listing__(path, skip_exist_check = skip_exist_check)
  }

  def __s3_get_directory_listing__(path: String, skip_exist_check: Boolean = false): Seq[String] = {
    S3Wrapper.get_directory_listing(path, skip_exist_check = skip_exist_check) 
  }

  def __local_get_directory_listing__(path: String, skip_exist_check: Boolean = false): Seq[String] = {
    LocalFSWrapper.get_directory_listing(path, skip_exist_check = skip_exist_check) 
  }

  def list_dirs(path: String): Seq[String] = {
    val path2 = __normalize_path__(path)
    val result1 = ls(path2)
    val result2 = result1.filter({ t => is_directory(path2 + "/" + t) })
    result2
  }

  def list_files(path: String, include_reserved_files: Boolean = false): Seq[String] = {
    val path2 = __normalize_path__(path)
    val result1 = ls(path, include_reserved_files = include_reserved_files)
    val result2 = result1.filter({ t => is_file(path2 + "/" + t) })
    result2
  }

  def is_file(path: String): Boolean = {
    is_directory(path) == false 
  }

  def is_directory(path: String): Boolean = {
    file_exists("%s/%s".format(path, RESERVED_HIDDEN_FILE))
  }

  def delete_file_with_wait(path: String, ignore_if_missing: Boolean, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): Boolean = {
    // check for ignore missing
    if (file_exists(path) == false) {
      if (ignore_if_missing == true) {
        Utils.debug("delete_file_with_wait: path doesnt exist. ignore_if_missing: %s, returning".format(ignore_if_missing))
        true
      } else {
        // check if attempts left
        if (attempts > 0) {
          Utils.info("delete_file_with_wait: path: %s doesnt exists. ignore_if_missing is False. attempts: %s, sleep for : %s seconds".format(path, attempts, wait_sec))
          Thread.sleep(wait_sec)
          delete_file_with_wait(path, ignore_if_missing = ignore_if_missing, wait_sec = wait_sec, attempts = attempts - 1)
        } else {
          Utils.info("delete_file_with_wait: path doesnt exists. ignore_if_missing is False. attempts: over")
          throw new RuntimeException("delete_file_with_wait: unable to delete file: %s".format(path))
        }
      }
    } else {
      // file exists. call delete
      delete_file(path, ignore_if_missing = ignore_if_missing)

      // verify that the file is deleted
      file_not_exists_with_wait(path)
    }
  }

  def delete_file(path: String, ignore_if_missing: Boolean = false) = {
    if (__is_s3__(path))
      __s3_delete_file__(path, ignore_if_missing = ignore_if_missing)
    else
      __local_delete_file__(path, ignore_if_missing = ignore_if_missing)
  }

  def __s3_delete_file__(path: String, ignore_if_missing: Boolean = false) = {
    S3Wrapper.delete_file(path, ignore_if_missing = ignore_if_missing)
  }

  def __local_delete_file__(path: String, ignore_if_missing: Boolean = false) = {
    // delete file
    LocalFSWrapper.delete_file(path, fail_if_missing = ignore_if_missing)

    // check if this was the reserved file for directory
    if (path.endsWith("/" + RESERVED_HIDDEN_FILE)) {
      // delete the directory
      val dir_path = path.take(path.lastIndexOf("/"))
      LocalFSWrapper.delete_dir(dir_path)
    }
  }

  def delete_dir_with_wait(path: String, ignore_if_missing: Boolean, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): Boolean = {
    val path2 = __normalize_path__(path)
    val file_path = "%s/%s".format(path, RESERVED_HIDDEN_FILE)

    // check for existence 
    if (file_exists(file_path) == false) {
      if (ignore_if_missing == true) {
        Utils.warn("delete_dir: path doesnt exist: %s, ignore_if_missing: %s".format(path, ignore_if_missing))
        return false 
      } else {
        throw new RuntimeException("delete_dir: path doesnt exist: %s".format(path))
      }
    }

    // check if the directory is empty or not. to prevent race conditions
    if (ls(path).length > 0) {
        Utils.warn("delete_dir: directory not empty: %s".format(path))
        return false
    }

    // delete the reserved file 
    delete_file_with_wait(file_path, ignore_if_missing = ignore_if_missing, wait_sec = wait_sec, attempts = attempts)
  }

  def get_parent_directory(path: String) = {
    // normalize
    val path2 = __normalize_path__(path)

    // get the last index
    val index = path.lastIndexOf("/")

    // return
    path.take(index)
  }

  def write_text_file(path: String, text: String) = {
    if (__is_s3__(path))
      __s3_write_text_file__(path, text)
    else
      __local_write_text_file__(path, text)
  }

  def __s3_write_text_file__(path: String, text: String) = {
    val path2 = __normalize_path__(path)
    val (bucket_name, object_key) = Utils.split_s3_path(path2)
    S3Wrapper.put_file_with_text_content(bucket_name, object_key, text)
  }

  def __local_write_text_file__(path: String, text: String) = {
    val path2 = __normalize_path__(path)
    LocalFSWrapper.put_file_with_text_content(path2, text)
  }

  def create_dir(path: String) = {
    Utils.debug("create_dir: %s".format(path))
    if (__is_s3__(path))
      __s3_create_dir__(path)
    else
      __local_create_dir__(path)
  }

  def __s3_create_dir__(path: String) = {
    val path2 = __normalize_path__(path)
    val file_path = "%s/%s".format(path, RESERVED_HIDDEN_FILE)
    val text = ""
    write_text_file(file_path, text)
  }

  def __local_create_dir__(path: String) = {
    val path2 = __normalize_path__(path)
    // create the directory
    LocalFSWrapper.makedirs(path2)

    // create the reserved file
    val file_path = "%s/%s".format(path, RESERVED_HIDDEN_FILE)
    val text = ""
    write_text_file(file_path, text)
  }

  def makedirs(path: String, levels: Int = 1) = {
    Utils.trace("makedirs: path: %s, levels: %s".format(path, levels))
    if (__is_s3__(path))
        __s3_makedirs__(path, levels = levels)
    else
        __local_makedirs__(path, levels = levels)
  }

  def __s3_makedirs__(path: String, levels: Int = -999) = {
    // normalize path
    val path2 = __normalize_path__(path)

    // split path into bucket and key
    val (bucket_name, object_key) = Utils.split_s3_path(path2)
    // TODO: silent bug probably. The split creates an empty string as first part for leading '/'
    val parts = object_key.split("/")

    // iterate over all ancestors
    Range(0, math.min(levels, parts.length)).foreach({ i =>
      // construct parent directory
      val parent_dir = "s3://%s/%s".format(bucket_name, parts.take(parts.length - levels + i + 1).mkString("/"))
      Utils.debug("__s3_makedirs__: path: %s, levels: %s, i: %s, parent_dir: %s".format(path, levels, i, parent_dir))

      // create the directory if it doesnt exist. This will cover path too
      if (is_directory(parent_dir) == false)
        create_dir(parent_dir)
    })
  }

  def __local_makedirs__(path: String, levels: Int = -999) = {
    // normalize path
    val path2 = __normalize_path__(path)
    // TODO: silent bug probably. The split creates an empty string as first part for leading '/'
    val parts = path2.split("/")

    // iterate over all ancestors
    Range(0, math.min(levels, parts.length)).foreach({ i =>
      // construct parent directory
      val parent_dir = parts.take(parts.length - levels + i + 1).mkString("/")
      val parent_dir2 = {
        if (parent_dir.startsWith("/") == false)
          "/" + parent_dir
        else
          parent_dir
      }

      // create the directory if it doesnt exist. This will cover path too
      if (is_directory(parent_dir2) == false)
          create_dir(parent_dir)
    })
  }

  def get_last_modified_timestamp(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_get_last_modified_timestamp__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_get_last_modified_timestamp__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def copy_leaf_dir(src_path: String, dest_path: String, overwrite: Boolean = false, append: Boolean = true) = {
    // check if src exists
    if (dir_exists(src_path) == false)
        throw new RuntimeException("copy_leaf_dir: src_path doesnt exist: %s".format(src_path))

    // // check if it is not a leaf dir
    // if (len(self.list_dirs(dest_path)) > 0):
    //     throw new RuntimeException("copy_leaf_dir: can not copy non leaf directories: %s".format(dest_path))

    // check if dest exists
    if (dir_exists(dest_path) == true)
        // check if overwrite is False
        if (overwrite == false)
            throw new RuntimeException("copy_leaf_dir: dest_path exists and overwrite = False: %s".format(dest_path))

    // create dir
    create_dir(dest_path)

    // gather the list of files to copy 
    val src_files = list_files(src_path)
    val dest_files = list_files(dest_path)

    // check if there are files that will not be overwritten
    val files_not_in_src = (Set(dest_files).diff(Set(src_files))).toSeq

    // check for append flag
    if (files_not_in_src.length > 0)
      if (append == false)
        throw new RuntimeException("copy_leaf_dir: there are files in dest that will not be replaced and append = False: %s".format(files_not_in_src))
      else
        // debug
        Utils.warn("copy_leaf_dir: there are files in dest that will not be replaced: %s".format(files_not_in_src))

    // iterate
    src_files.foreach({ f =>
      // debug
      Utils.info("copy_leaf_dir: copying: %s".format(f))

      // if file exists in dest, warn
      if (file_exists("%s/%s".format(dest_path, f)))
          Utils.warn("copy_leaf_dir: overwriting existing file: %s".format(f))

      // copy
      val contents = read_file_contents_as_text("%s/%s".format(src_path, f))
      write_text_file("%s/%s".format(dest_path, f), contents)
    })
  }

  def list_leaf_dir(path: String, include_reserved_files: Boolean = false, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS) = {
    ls(path, include_reserved_files = include_reserved_files, wait_sec = wait_sec, attempts = attempts, skip_exist_check = true)
  }
}

