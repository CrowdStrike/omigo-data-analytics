from omigo_core import s3_wrapper, local_fs_wrapper
from omigo_core import tsv
from omigo_core import utils
import time
import datetime
from datetime import timezone
import os

# TODO: look for aws boto3 response code to make sure the api ran
# TODO: This should be moved to omigo-core
RESERVED_HIDDEN_FILE = ".omigo.ignore"
WAIT_SEC = 1

DEFAULT_WAIT_SEC = 3
DEFAULT_ATTEMPTS = 3

class S3FSWrapper:
    def __init__(self):
        pass

    def __is_s3__(self, path):
        if (path.startswith("s3://")):
            return True
        else:
            return False

    def file_exists(self, path):
        if (self.__is_s3__(path)):
            return self.__s3_file_exists__(path)
        else:
            return self.__local_file_exists__(path)

    def __s3_file_exists__(self, path):
        path = self.__normalize_path__(path)
        return s3_wrapper.check_file_exists(path)

    def __local_file_exists__(self, path):
        return local_fs_wrapper.check_file_exists(path)

    def file_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        # check if exists
        if (self.file_exists(path)):
            return True
     
        # wait on false 
        if (attempts > 0):
            utils.info("exists_with_wait: path: {}, attempts: {}, waiting for {} seconds".format(path, attempts, wait_sec))
            time.sleep(wait_sec)
            return self.file_exists_with_wait(path, wait_sec = wait_sec, attempts = attempts - 1)
        else:
            return False

    def dir_exists(self, path):
        dir_file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)
        return self.file_exists(dir_file_path)

    def dir_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        dir_file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)
        return self.file_exists_with_wait(dir_file_path, wait_sec = wait_sec, attempts = attempts)

    def file_not_exists(self, path):
        if (self.__is_s3__(path)):
            return self.__s3_file_not_exists__(path)
        else:
            return self.__local_file_not_exists__(path)

    def __s3_file_not_exists__(self, path):
        path = self.__normalize_path__(path)
        return s3_wrapper.check_file_exists(path) == False

    def __local_file_not_exists__(self, path):
        path = self.__normalize_path__(path)
        return local_fs_wrapper.check_file_exists(path) == False

    def file_not_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        # check if exists
        if (self.file_not_exists(path)):
            return True
     
        # wait on false 
        if (attempts > 0):
            utils.info("not_exists_with_wait: path: {}, attempts: {}, waiting for {} seconds".format(path, attempts, wait_sec))
            time.sleep(wait_sec)
            return self.file_not_exists_with_wait(path, wait_sec = wait_sec, attempts = attempts - 1)
        else:
            return False

    def dir_not_exists_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        dir_file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)
        return self.file_not_exists_with_wait(dir_file_path, wait_sec = wait_sec, attempts = attempts)

    def read_file_contents_as_text_with_wait(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        path = self.__normalize_path__(path)

        # go into a wait loop for s3 to sync if the path is missing
        if (self.file_exists(path) == False and attempts > 0):
            utils.info("read: path: {} doesnt exist. waiting for {} seconds. attempts: {}".format(path, wait_sec, attempts))
            time.sleep(wait_sec)
            return self.read_file_contents_as_text_with_wait(path, wait_sec = wait_sec, attempts = attempts - 1)

        # return
        return self.read_file_contents_as_text(path)

    def read_file_contents_as_text(self, path):
        if (self.__is_s3__(path)):
            return self.__s3_read_file_contents_as_text__(path)
        else:
            return self.__local_read_file_contents_as_text__(path)

    def __s3_read_file_contents_as_text__(self, path):
        path = self.__normalize_path__(path)
        bucket_name, object_key = utils.split_s3_path(path)
        return s3_wrapper.get_file_content_as_text(bucket_name, object_key)

    # the path here can be compressed gz file
    def __local_read_file_contents_as_text__(self, path):
        path = self.__normalize_path__(path)
        return local_fs_wrapper.get_file_content_as_text(path)

    def __normalize_path__(self, path):
        if (path.endswith("/")):
            path = path[0:-1]

        if (path.endswith("/")):
            raise Exception("Multiple trailing '/' characters found: {}".format(path))

        return path

    def __simplify_dir_list__(self, path, listings, include_reserved_files = False):
        path = self.__normalize_path__(path)

        # get index to simplify output
        index = len(path)
        result1 = list(filter(lambda t: include_reserved_files == True or t.endswith("/" + RESERVED_HIDDEN_FILE) == False, listings))
        result2 = list(map(lambda t: t[index + 1:], result1))

        # return
        return result2

    # TODO: this is a wait method and confusing
    def ls(self, path, include_reserved_files = False, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS, skip_exist_check = False):
        path = self.__normalize_path__(path)

        # go into a wait loop for s3 to sync if the path is missing
        if (self.dir_exists(path) == False and attempts > 0):
            utils.info("ls: path: {} doesnt exist. waiting for {} seconds. attempts: {}".format(path, wait_sec, attempts))
            time.sleep(wait_sec)
            return self.ls(path, include_reserved_files = include_reserved_files, wait_sec = wait_sec, attempts = attempts - 1)

        # get directory listings
        listings = self.get_directory_listing(path, skip_exist_check = skip_exist_check)
        return self.__simplify_dir_list__(path, listings, include_reserved_files = include_reserved_files)


    def get_directory_listing(self, path, skip_exist_check = False):
        if (self.__is_s3__(path)):
            return self.__s3_get_directory_listing__(path, skip_exist_check = skip_exist_check)
        else:
            return self.__local_get_directory_listing__(path, skip_exist_check = skip_exist_check)

    def __s3_get_directory_listing__(self, path, skip_exist_check = False):
        return s3_wrapper.get_directory_listing(path, skip_exist_check = skip_exist_check)

    def __local_get_directory_listing__(self, path, skip_exist_check = False):
        return local_fs_wrapper.get_directory_listing(path, skip_exist_check = skip_exist_check)

    def list_dirs(self, path):
        path = self.__normalize_path__(path)
        result1 = self.ls(path)
        result2 = list(filter(lambda t: self.is_directory(path + "/" + t), result1))
        return result2

    def list_files(self, path, include_reserved_files = False):
        path = self.__normalize_path__(path)
        result1 = self.ls(path, include_reserved_files = include_reserved_files)
        result2 = list(filter(lambda t: self.is_file(path + "/" + t), result1))
        return result2

    def is_file(self, path):
        return self.is_directory(path) == False

    def is_directory(self, path):
        return self.file_exists("{}/{}".format(path, RESERVED_HIDDEN_FILE))

    # TODO: confusing logic
    def delete_file_with_wait(self, path, ignore_if_missing = True, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        # check for ignore missing
        if (self.file_exists(path) == False):
            if (ignore_if_missing == True):
                utils.debug("delete_file_with_wait: path doesnt exist. ignore_if_missing: {}, returning".format(ignore_if_missing))
                return True
            else:
                # check if attempts left
                if (attempts > 0):
                    utils.info("delete_file_with_wait: path: {} doesnt exists. ignore_if_missing is False. attempts: {}, sleep for : {} seconds".format(path, attempts, wait_sec))
                    time.sleep(wait_sec)
                    return self.delete_file_with_wait(path, ignore_if_missing = ignore_if_missing, wait_sec = wait_sec, attempts = attempts - 1)
                else:
                    utils.info("delete_file_with_wait: path doesnt exists. ignore_if_missing is False. attempts: over")
                    raise Exception("delete_file_with_wait: unable to delete file: {}".format(path))
        else:
            # file exists. call delete
            self.delete_file(path, ignore_if_missing = ignore_if_missing)

            # verify that the file is deleted
            return self.file_not_exists_with_wait(path)

    def delete_file(self, path, ignore_if_missing = False):
        if (self.__is_s3__(path)):
            return self.__s3_delete_file__(path, ignore_if_missing = ignore_if_missing)
        else:
            return self.__local_delete_file__(path, ignore_if_missing = ignore_if_missing)

    def __s3_delete_file__(self, path, ignore_if_missing = False):
        return s3_wrapper.delete_file(path, ignore_if_missing = ignore_if_missing)

    def __local_delete_file__(self, path, ignore_if_missing = False):
        # delete file
        local_fs_wrapper.delete_file(path, fail_if_missing = ignore_if_missing)

        # check if this was the reserved file for directory
        if (path.endswith("/" + RESERVED_HIDDEN_FILE)):
            # delete the directory
            dir_path = path[0:path.rindex("/")]
            local_fs_wrapper.delete_dir(dir_path)

    def delete_dir_with_wait(self, path, ignore_if_missing = True, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        path = self.__normalize_path__(path)
        file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)

        # check for existence 
        if (self.file_exists(file_path) == False):
            if (ignore_if_missing == True):
                utils.warn("delete_dir: path doesnt exist: {}, ignore_if_missing: {}".format(path, ignore_if_missing))
                return False
            else:
                raise Exception("delete_dir: path doesnt exist: {}".format(path))

        # check if the directory is empty or not. to prevent race conditions
        if (len(self.ls(path)) > 0):
            utils.warn("delete_dir: directory not empty: {}".format(path))
            return False

        # delete the reserved file 
        return self.delete_file_with_wait(file_path, ignore_if_missing = ignore_if_missing, wait_sec = wait_sec, attempts = attempts)

    def get_parent_directory(self, path):
        # normalize
        path = self.__normalize_path__(path)

        # get the last index
        index = path.rindex("/")

        # return
        return path[0:index]

    def write_text_file(self, path, text):
        if (self.__is_s3__(path)):
            return self.__s3_write_text_file__(path, text)
        else:
            return self.__local_write_text_file__(path, text)

    def __s3_write_text_file__(self, path, text):
        path = self.__normalize_path__(path)
        bucket_name, object_key = utils.split_s3_path(path)
        s3_wrapper.put_file_with_text_content(bucket_name, object_key, text)

    def __local_write_text_file__(self, path, text):
        path = self.__normalize_path__(path)
        local_fs_wrapper.put_file_with_text_content(path, text)

    def create_dir(self, path):
        utils.debug("create_dir: {}".format(path))
        if (self.__is_s3__(path)):
            self.__s3_create_dir__(path)
        else:
            self.__local_create_dir__(path)

    def __s3_create_dir__(self, path):
        path = self.__normalize_path__(path)
        file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)
        text = ""
        self.write_text_file(file_path, text)

    def __local_create_dir__(self, path):
        path = self.__normalize_path__(path)
        # create the directory
        local_fs_wrapper.makedirs(path)

        # create the reserved file
        file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)
        text = ""
        self.write_text_file(file_path, text)

    def makedirs(self, path, levels = 1):
        utils.trace("makedirs: path: {}, levels: {}".format(path, levels))
        if (self.__is_s3__(path)):
            self.__s3_makedirs__(path, levels = levels)
        else:
            self.__local_makedirs__(path, levels = levels)

    def __s3_makedirs__(self, path, levels = None):
        # normalize path
        path = self.__normalize_path__(path)

        # split path into bucket and key
        bucket_name, object_key = utils.split_s3_path(path)
        # TODO: silent bug probably. The split creates an empty string as first part for leading '/'
        parts = object_key.split("/")

        # iterate over all ancestors
        for i in range(min(levels, len(parts))):
            # construct parent directory
            parent_dir = "s3://{}/{}".format(bucket_name, "/".join(parts[0:(len(parts) - levels + i + 1)]))
            utils.debug("__s3_makedirs__: path: {}, levels: {}, i: {}, parent_dir: {}".format(path, levels, i, parent_dir))

            # create the directory if it doesnt exist. This will cover path too
            if (self.is_directory(parent_dir) == False):
                self.create_dir(parent_dir)

    def __local_makedirs__(self, path, levels = None):
        # normalize path
        path = self.__normalize_path__(path)
        # TODO: silent bug probably. The split creates an empty string as first part for leading '/'
        parts = path.split("/")

        # iterate over all ancestors
        for i in range(min(levels, len(parts))):
            # construct parent directory
            parent_dir = "/".join(parts[0:(len(parts) - levels + i + 1)])
            if (parent_dir.startswith("/") == False):
                parent_dir = "/" + parent_dir

            # create the directory if it doesnt exist. This will cover path too
            if (self.is_directory(parent_dir) == False):
                self.create_dir(parent_dir)

    def get_last_modified_timestamp(self, path):
        if (self.__is_s3__(path)):
            return self.__s3_get_last_modified_timestamp__(path)
        else:
            return self.__local_get_last_modified_timestamp__(path)

    def __s3_get_last_modified_timestamp__(self, path):
        # normalize
        path = self.__normalize_path__(path)
        dt = s3_wrapper.get_last_modified_time(path)

        # return
        return int(dt.replace(tzinfo = timezone.utc).timestamp())

    def __local_get_last_modified_timestamp__(self, path):
        return local_fs_wrapper.get_last_modified_timestamp(path)

    def copy_leaf_dir(self, src_path, dest_path, overwrite = False, append = True):
        # check if src exists
        if (self.dir_exists(src_path) == False):
            raise Exception("copy_leaf_dir: src_path doesnt exist: {}".format(src_path))

        # # check if it is not a leaf dir
        # if (len(self.list_dirs(dest_path)) > 0):
        #     raise Exception("copy_leaf_dir: can not copy non leaf directories: {}".format(dest_path))

        # check if dest exists
        if (self.dir_exists(dest_path) == True):
            # check if overwrite is False
            if (overwrite == False):
                raise Exception("copy_leaf_dir: dest_path exists and overwrite = False: {}".format(dest_path))

        # create dir
        self.create_dir(dest_path)

        # gather the list of files to copy 
        src_files = self.list_files(src_path)
        dest_files = self.list_files(dest_path)

        # check if there are files that will not be overwritten
        files_not_in_src = list(set(dest_files).difference(set(src_files)))

        # check for append flag
        if (len(files_not_in_src) > 0):
            if (append == False):
                raise Exception("copy_leaf_dir: there are files in dest that will not be replaced and append = False: {}".format(files_not_in_src))
            else:
                # debug
                utils.warn("copy_leaf_dir: there are files in dest that will not be replaced: {}".format(files_not_in_src))

        # iterate
        for f in src_files:
            # debug
            utils.info("copy_leaf_dir: copying: {}".format(f))

            # if file exists in dest, warn
            if (self.file_exists("{}/{}".format(dest_path, f))):
                utils.warn("copy_leaf_dir: overwriting existing file: {}".format(f))

            # copy
            contents = self.read_file_contents_as_text("{}/{}".format(src_path, f))
            self.write_text_file("{}/{}".format(dest_path, f), contents)

    def list_leaf_dir(self, path, include_reserved_files = False, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        return self.ls(path, include_reserved_files = include_reserved_files, wait_sec = wait_sec, attempts = attempts, skip_exist_check = True)
