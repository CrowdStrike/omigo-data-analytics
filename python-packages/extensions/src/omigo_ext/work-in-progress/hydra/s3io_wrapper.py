# Wrapper for s3fs package to work with S3
from omigo_core import s3_wrapper
from omigo_core import tsv
from omigo_core import utils
import time
import datetime
from datetime import timezone

# TODO: This should be moved to omigo-core
RESERVED_HIDDEN_FILE = ".omigo.ignore"
WAIT_SEC = 1

DEFAULT_WAIT_SEC = 3
DEFAULT_ATTEMPTS = 3

class S3FSWrapper:
    def __init__(self):
        pass

    def file_exists(self, path):
        path = self.__normalize_path__(path)
        return s3_wrapper.check_file_exists(path)

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
        path = self.__normalize_path__(path)
        return s3_wrapper.check_file_exists(path) == False

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

    def read_file_contents_as_text(self, path, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        path = self.__normalize_path__(path)

        # go into a wait loop for s3 to sync if the path is missing
        if (self.file_exists(path) == False and attempts > 0):
            utils.info("read: path: {} doesnt exist. waiting for {} seconds. attempts: {}".format(path, wait_sec, attempts))
            time.sleep(wait_sec)
            return self.read(path, wait_sec = wait_sec, attempts = attempts - 1)

        bucket_name, object_key = utils.split_s3_path(path)
        return s3_wrapper.get_s3_file_content_as_text(bucket_name, object_key)

    def __normalize_path__(self, path):
        if (path.endswith("/")):
            path = path[0:-1]

        if (path.endswith("/")):
            raise Exception("Multiple trailing '/' characters found: {}".format(path))

        return path

    def __simplify_dir_list__(self, path, listings, include_reserved_files = False):
        path = self.__normalize_path__(path)

        index = len(path)
        result1 = list(filter(lambda t: include_reserved_files == True or t.endswith("/" + RESERVED_HIDDEN_FILE) == False, listings))
        result2 = list(map(lambda t: t[index + 1:], result1))

        # return
        return result2

    def ls(self, path, include_reserved_files = False, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        path = self.__normalize_path__(path)

        # go into a wait loop for s3 to sync if the path is missing
        if (self.dir_exists(path) == False and attempts > 0):
            utils.info("ls: path: {} doesnt exist. waiting for {} seconds. attempts: {}".format(path, wait_sec, attempts))
            time.sleep(wait_sec)
            return self.ls(path, include_reserved_files = include_reserved_files, wait_sec = wait_sec, attempts = attempts - 1)

        listings = s3_wrapper.get_directory_listing(path)
        return self.__simplify_dir_list__(path, listings, include_reserved_files = include_reserved_files)

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
    def delete_file(self, path, ignore_missing = True, wait_sec = DEFAULT_WAIT_SEC, attempts = DEFAULT_ATTEMPTS):
        exists_flag = self.file_exists(path)

        # check for ignore missing
        if (self.file_exists(path) == False):
            if (ignore_missing == True):
                utils.debug("delete_file: path doesnt exist. ignore_missing: {}, returning".format(ignore_missing))
                return True
            else:
                # check if attempts left
                if (attempts > 0):
                    utils.info("delete_file: path: {} doesnt exists. ignore_missing is False. attempts: {}, sleep for : {} seconds".format(path, attempts, wait_sec))
                    time.sleep(wait_sec)
                    return self.delete_file(path, ignore_missing = ignore_missing, wait_sec = wait_sec, attempts = attempts - 1)
                else:
                    utils.info("delete_file: path doesnt exists. ignore_missing is False. attempts: over")
                    raise Exception("delete_file: unable to delete file: {}".format(path))
        else:
            # file exists. call delete
            s3_wrapper.delete_file(path, fail_if_missing = ignore_missing)

            # verify that the file is deleted
            return self.file_not_exists_with_wait(path)

    def delete_dir(self, path, ignore_missing = True):
        path = self.__normalize_path__(path)
        file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)

        # check for existence 
        if (self.file_exists(file_path) == False):
            if (ignore_missing == True):
                utils.warn("delete_dir: path doesnt exist: {}, ignore_missing: {}".format(path, ignore_missing))
                return
            else:
                raise Exception("delete_dir: path doesnt exist: {}".format(path))

        # check if the directory is empty or not. to prevent race conditions
        if (len(self.ls(path)) > 0):
            utils.warn("delete_dir: directory not empty: {}".format(path))
            return False

        # delete the reserved file 
        self.delete_file(file_path, ignore_missing = ignore_missing)
        return True

    def get_parent_directory(self, path):
        # normalize
        path = self.__normalize_path__(path)

        # get the last index
        index = path.rindex("/")

        # return
        return path[0:index]

    def write_text_file(self, path, text):
        path = self.__normalize_path__(path)
        bucket_name, object_key = utils.split_s3_path(path)
        s3_wrapper.put_s3_file_with_text_content(bucket_name, object_key, text)

    def create_dir(self, path):
        path = self.__normalize_path__(path)
        file_path = "{}/{}".format(path, RESERVED_HIDDEN_FILE)
        text = ""
        self.write_text_file(file_path, text)

    def makedirs(self, path):
        # normalize path
        path = self.__normalize_path__(path)
        parts = path.split("/")

        # split path into bucket and key
        bucket_name, object_key = utils.split_s3_path(path)
        parts = object_key.split("/")

        # iterate over all ancestors
        for i in range(len(parts)):
            # construct parent directory
            parent_dir = "s3://{}/{}".format(bucket_name, "/".join(parts[0:i+1]))

            # create the directory if it doesnt exist. This will cover path too
            if (self.is_directory(parent_dir) == False):
                self.create_dir(parent_dir)

    def get_last_modified_timestamp(self, path):
        # normalize
        path = self.__normalize_path__(path)
        dt = s3_wrapper.get_last_modified_time(path)

        # return
        return int(dt.replace(tzinfo = timezone.utc).timestamp())
