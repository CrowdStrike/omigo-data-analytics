import os
import datetime
from omigo_core import funclib
from omigo_core import utils
import zipfile, gzip

def check_path_exists(path):
    return os.path.exists(path)

def check_file_exists(path):
    return os.path.exists(path)

def get_directory_listing(path, filter_func = None, fail_if_missing = True, skip_exist_check = False):
    # skip_exist_check is for performance and parity with s3
    if (skip_exist_check == False):
        if (check_path_exists(path) == False):
            if (fail_if_missing):
                raise Exception("Directory does not exist: {}".format(path))
            else:
                return []

    full_paths = []
    for p in os.listdir(path):
        full_paths.append(path + "/" + p)

    # apply filter func if any
    if (filter_func is not None):
        full_paths = list(filter(lambda t: filter_func(t), full_paths))

    # return
    return full_paths

# TODO: not in s3
def makedirs(path, exist_ok = True):
    return os.makedirs(path, exist_ok = exist_ok)

def get_file_content_as_text(path):
    # initialize
    data = None

    # check for file exists
    if (check_path_exists(path) == False):
        raise Exception("file doesnt exist: {}".format(path))

    # read based on file type
    if (path.endswith(".gz")):
        fin = gzip.open(path, mode = "rt")
        data = [x.rstrip("\n") for x in fin.readlines()]
        fin.close()
    elif (path.endswith(".zip")):
        zipf = zipfile.ZipFile(path, "r")
        fin = zipf.open(zipf.infolist()[0], "r")
        data = fin.read().decode().split("\n")
        fin.close()
        zipf.close()
    else:
        fin = open(path, "r")
        data = [x.rstrip("\n") for x in fin.readlines()]
        fin.close()

    # return
    return "\n".join(data)

def get_last_modified_timestamp(path):
    return funclib.datetime_to_utctimestamp(datetime.datetime.utcfromtimestamp(int(os.path.getmtime(path))))

# TODO: this is not tested
def put_file_with_text_content(path, text):
    utils.warn_once("put_file_with_text_content is not tested fully")

    # initialize
    output_zipf = None
    output_file = None

    # construct output file
    if (path.endswith(".gz")):
        output_file = gzip.open(path, "wt")
    elif (path.endswith(".zip")):
        output_zipf = zipfile.ZipFile(path, "wt")
        output_file = output_zipf.open(path.split("/")[-1][0:-4], "w")
    else:
        output_file = open(path, "wt")

    # write
    output_file.write(text)

    # close
    if (output_zipf is not None):
        output_zipf.close()
    if (output_file is not None):
        output_file.close()

# TODO: return success status
def delete_file(path, fail_if_missing = False):
    utils.debug("delete_file: path: {}, fail_if_missing: {}".format(path, fail_if_missing))
    # check if the file exists
    if (check_path_exists(path) == False):
        if (fail_if_missing):
            raise Exception("delete_file: path doesnt exist: {}".format(path))
        else:
            utils.debug("delete_file: path doesnt exist: {}".format(path))

        return

    # delete
    os.remove(path)
  
# TODO: This api doesnt have s3 counterpart 
def delete_dir(path, fail_if_missing = False):
    utils.warn_once("delete_dir: this api doesnt have s3 counterpart")
    utils.debug("delete_dir: path: {}, fail_if_missing: {}".format(path, fail_if_missing))
    # check if the file exists
    if (check_path_exists(path) == False):
        if (fail_if_missing):
            raise Exception("delete_dir: path doesnt exist: {}".format(path))
        else:
            utils.debug("delete_dir: path doesnt exist: {}".format(path))

        return

    # delete
    os.rmdir(path)
