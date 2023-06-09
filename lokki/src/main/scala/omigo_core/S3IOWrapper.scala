package omigo_core

object S3IOWrapper {
  val RESERVED_HIDDEN_FILE = ".omigo.ignore"
  val WAIT_SEC = 1

  val DEFAULT_WAIT_SEC = 3
  val DEFAULT_ATTEMPTS = 3
}

class S3IOWrapper {
  def __is_s3__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def file_exists(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_file_exists__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_file_exists__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def file_exists_with_wait(path: String, wait_sec: Int, attempts: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def dir_exists(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def dir_exists_with_wait(path: String, wait_sec: Int, attempts: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def file_not_exists(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_file_not_exists__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_file_not_exists__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def file_not_exists_with_wait(path: String, wait_sec: Int, attempts: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def dir_not_exists_with_wait(path: String, wait_sec: Int, attempts: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def read_file_contents_as_text_with_wait(path: String, wait_sec: Int, attempts: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def read_file_contents_as_text(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_read_file_contents_as_text__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_read_file_contents_as_text__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __normalize_path__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __simplify_dir_list__(path: String, listings: Seq[String], include_reserved_files: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def ls(path: String, include_reserved_files: Boolean, wait_sec: Int, attempts: Int, skip_exist_check: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def get_directory_listing(path: String, skip_exist_check: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __s3_get_directory_listing__(path: String, skip_exist_check: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def __local_get_directory_listing__(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def list_dirs(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def list_files(path: String, include_reserved_files: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def is_file(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def is_directory(path: String) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def delete_file_with_wait(path: String, ignore_if_missing: Boolean, wait_sec: Int, attempts: Int) = {
    throw new RuntimeException("Not Implemented Yet")
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

  def delete_dir_with_wait(path: String, ignore_if_missing: Boolean, wait_sec: Int, attempts: Int) = {
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

  def copy_leaf_dir(src_path: String, dest_path: String, overwrite: Boolean, append: Boolean) = {
    throw new RuntimeException("Not Implemented Yet")
  }

  def list_leaf_dir(path: String, include_reserved_files: Boolean, wait_sec: Int, attempts: Int) = {
    throw new RuntimeException("Not Implemented Yet")
  }
}

