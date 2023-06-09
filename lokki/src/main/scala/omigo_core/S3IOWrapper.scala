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

  def delete_file(path: String, ignore_if_missing: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_delete_file__(path: String, ignore_if_missing: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_delete_file__(path: String, ignore_if_missing: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def delete_dir_with_wait(path: String, ignore_if_missing: Boolean, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS): Boolean = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def get_parent_directory(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def write_text_file(path: String, text: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_write_text_file__(path: String, text: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_write_text_file__(path: String, text: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def create_dir(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_create_dir__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_create_dir__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def makedirs(path: String,levels: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_makedirs__(path: String, levels: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_makedirs__(path: String, levels: Int) = {
    throw new RuntimeException("Not Implemented Yet")
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
    throw new RuntimeException("Not Implemented Yet")
  }

  def list_leaf_dir(path: String, include_reserved_files: Boolean = false, wait_sec: Int = DEFAULT_WAIT_SEC, attempts: Int = DEFAULT_ATTEMPTS) = {
    throw new RuntimeException("Not Implemented Yet")
  }
}

