"""Utility methods to work with file read and write"""

import os
import gzip
import datetime
import zipfile

# local imports
from omigo_core import utils
from omigo_core import timefuncs
from omigo_hydra import s3io_wrapper
# constant
NUM_HOURS = 24

# method to read the data
def read_filepaths(path, start_date_str, end_date_str, fileprefix, s3_region = None, aws_profile = None, granularity = "hourly", ignore_missing = False):
    if (granularity == "hourly"):
        return read_filepaths_hourly(path, start_date_str, end_date_str, fileprefix, s3_region = s3_region, aws_profile = aws_profile, etl_level = "", ignore_missing = ignore_missing)
    elif (granularity == "daily"):
        return read_filepaths_daily(path, start_date_str, end_date_str, fileprefix, s3_region = s3_region, aws_profile = aws_profile, etl_level = "", ignore_missing = ignore_missing)
    else:
        raise Exception("Unknown granularity value", granularity)

# this returns the etl prefix for creating directory depth
def get_etl_level_prefix(curdate, etl_level):
    prefix = "/"
    if (etl_level == ""):
        return prefix

    parts = etl_level.split(",")
    for part in parts:
        if (part == "year"):
            f = "%Y"
        elif (part == "month"):
            f = "%m"
        elif (part == "day"):
            f = "%d"
        else:
            raise Exception("Invalid value for etl_level :", etl_level, part)

        prefix = prefix + part + "-" + str(curdate.strftime(f)) + "/"

    return prefix

def read_filepaths_hourly(path, start_date_str, end_date_str, fileprefix, s3_region = None, aws_profile = None, etl_level = "", ignore_missing = False):
    utils.warn_once("read_filepaths_hourly is confusing and may be unsupported")
    # parse input dates
    start_date = datetime.datetime.strptime(start_date_str,"%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_str,"%Y-%m-%d")

    # construct paths based on the dates
    duration = end_date - start_date
    # print("read_filepaths_hourly: Number of days:", duration.days + 1)

    # get the list of file paths
    filepaths = []

    # loop through each day and construct the path
    for i in range(duration.days + 1):
        for j in range(NUM_HOURS):
            curdatetime = start_date + datetime.timedelta(days = i) + datetime.timedelta(hours = j)
            etl_prefix = get_etl_level_prefix(curdatetime, etl_level)
            filepath_tsv = path + etl_prefix + fileprefix + "-" + curdatetime.strftime("%Y%m%d-%H0000") + ".tsv"
            filepath_tsvgz = filepath_tsv + ".gz"
            filepath_tsvzip = filepath_tsv + ".zip"

            if (s3_wrapper.file_exists(filepath_tsv, s3_region = s3_region, aws_profile = aws_profile)):
                filepaths.append(filepath_tsv)
            elif (s3_wrapper.file_exists(filepath_tsvgz, s3_region = s3_region, aws_profile = aws_profile)):
                filepaths.append(filepath_tsvgz)
            elif (s3_wrapper.file_exists(filepath_tsvzip, s3_region = s3_region, aws_profile = aws_profile)):
                filepaths.append(filepath_tsvzip)
            else:
                if (ignore_missing == False):
                    raise Exception("Input files don't exist. Use ignore_missing if want to continue: ", filepath_tsv, filepath_tsvgz, filepath_tsvzip)
                else:
                    continue

    # return filepaths
    return filepaths

# Deprecated
def check_exists(path, s3_region = None, aws_profile = None):
    if (path.startswith("s3://") and s3_wrapper.check_path_exists(path, s3_region = s3_region, aws_profile = aws_profile)):
        return True

    if (local_fs_wrapper.check_path_exists(path)):
        return True

    return False

def read_filepaths_daily(path, start_date_str, end_date_str, fileprefix, s3_region = None, aws_profile = None, etl_level = "", ignore_missing = False):
    utils.warn_once("read_filepaths_daily is confusing and may be unsupported")

    # initialize fs
    fs = s3io_wrapper.S3FSWrapper(s3_region = s3_region, aws_profile = aws_profile)

    # parse input dates
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")

    # construct paths based on the dates
    duration = end_date - start_date
    #print("read_filepaths_daily: Number of days:", duration.days + 1)

    # get the list of file paths
    filepaths = []

    # loop through each day and construct the path
    for i in range(duration.days + 1):
        curdate = start_date + datetime.timedelta(days = i)
        etl_prefix = get_etl_level_prefix(curdate, etl_level)
        filepath_tsv = path + etl_prefix + fileprefix + "-" + curdate.strftime("%Y%m%d") + "-" + curdate.strftime("%Y%m%d") + ".tsv"
        filepath_tsvgz = filepath_tsv + ".gz"

        if (fs.file_exists(filepath_tsv, s3_region = s3_region, aws_profile = aws_profile)):
            filepaths.append(filepath_tsv)
        elif (fs.file_exists(filepath_tsvgz, s3_region = s3_region, aws_profile = aws_profile)):
            filepaths.append(filepath_tsvgz)
        else:
            if (ignore_missing == False):
                raise Exception("Input files don't exist. Use ignore_missing if want to continue: ", filepath_tsv, filepath_tsvgz)
            else:
                continue

    # return filepaths
    return filepaths

# check if the files in the filepaths have the same header
def has_same_headers(filepaths, s3_region = None, aws_profile = None):
    # headers set
    header_set = {}

    # read the headers to make sure that all files are same
    for filepath in filepaths:
        # print(filepath)

        # read content
        lines = read_file_content_as_lines(filepath, s3_region = s3_region, aws_profile = aws_profile)

        # read header
        headerline = lines[0].rstrip("\n")
        if ((headerline in header_set.keys()) == False):
            header_set[headerline] = filepath

    # check for no data
    if (len(header_set) == 0):
        print("Error in reading the files. No content.")
        return False

    # check for multiple headers
    if (len(header_set) > 1):
        print("Multiple headers found for the date range. Use a different date range.")
        for k, v in header_set.items():
            print("Path:", v, ", header:", k, "\n")
        return False

    # return all the filepaths
    return True

# create a hashmap of header fields
def create_header_map(header):
    header_map = {}
    parts = header.split("\t")
    for i in range(len(parts)):
        header_map[parts[i]] = i

    return header_map

def create_header_index_map(header):
    header_map = {}
    parts = header.split("\t")
    for i in range(len(parts)):
        header_map[i] = parts[i]

    return header_map

def read_file_content_as_lines(path, s3_region = None, aws_profile = None):
    # initialize fs
    fs = s3io_wrapper.S3FSWrapper(s3_region = s3_region, aws_profile = aws_profile)

    # read
    data = fs.read_file_contents_as_text(path)
    return data.split("\n")

def create_date_numeric_representation(date_str, default_suffix):
    # check for yyyy-MM-dd
    if (len(date_str) == 10):
        return str(date_str.replace("-", "") + default_suffix)
    else:
        return timefuncs.datestr_to_datetime(date_str).strftime("%Y%m%d%H%M%S")

# this is not a lookup function. This reads directory listing, and then picks the filepaths that match the criteria
def get_file_paths_by_datetime_range(path, start_date_str, end_date_str, prefix, spillover_window = 1, num_par = 10, wait_sec = 1, s3_region = None, aws_profile = None):
    # initialize fs
    fs = s3io_wrapper.S3FSWrapper(s3_region = s3_region, aws_profile = aws_profile)

    # parse dates
    start_date = timefuncs.datestr_to_datetime(start_date_str)
    end_date = timefuncs.datestr_to_datetime(end_date_str)

    # get number of days inclusive start and end and include +/- 1 day buffer for overlap
    num_days = (end_date - start_date).days + 1 + (spillover_window * 2)
    start_date_minus_window = start_date - datetime.timedelta(days = spillover_window)

    # create a numeric representation of date
    start_date_numstr = create_date_numeric_representation(start_date_str, "000000")
    end_date_numstr = create_date_numeric_representation(end_date_str, "999999")

    # create variable to store results
    tasks = []

    # iterate and create tasks
    for d in range(num_days):
        # generate the current path based on date
        cur_date = start_date_minus_window + datetime.timedelta(days = d)
        cur_path = path + "/dt=" + cur_date.strftime("%Y%m%d")

        # get the list of files. This needs to be failsafe as not all directories may exist
        tasks.append(utils.ThreadPoolTask(fs.get_directory_listing, cur_path, filter_func = None, ignore_if_missing = False, skip_exist_check = True,
            region = s3_region, profile = aws_profile))

    # execute the tasks
    results = utils.run_with_thread_pool(tasks, num_par = num_par, wait_sec = wait_sec)

    # final result
    paths_found = []

    # iterate over results
    for files_list in results:
        # debug
        utils.trace("file_paths_util: get_file_paths_by_datetime_range: number of candidate files to read: cur_date: {}, count: {}".format(cur_date, len(files_list)))

        # apply filter on the name and the timestamp
        for filename in files_list:
            #format: full_prefix/fileprefix-startdate-enddate-starttime-endtime.tsv
            # get the last part after /
            #sep_index = filename.rindex("/")
            #filename1 = filename[sep_index + 1:]
            base_filename = filename[len(cur_path) + 1:]
            ext_index = None

            # ignore any hidden files that start with dot(.)
            if (base_filename.startswith(".")):
                utils.trace("file_paths_util: get_file_paths_by_datetime_range: found hidden file. ignoring: {}".format(filename))
                continue

            # get extension
            if (base_filename.endswith(".tsv.gz")):
                ext_index = base_filename.rindex(".tsv.gz")
            elif (base_filename.endswith(".tsv")):
                ext_index = base_filename.rindex(".tsv")
            else:
                raise Exception("file_paths_util: get_file_paths_by_datetime_range: extension parsing failed: {}".format(filename))

            # proceed only if valid filename
            if (ext_index != -1):
                # strip the extension
                filename2 = base_filename[0:ext_index]
                filename3 = filename2[len(prefix) + 1:]
                parts = filename3.split("-")

                # the number of parts must be 3
                if (len(parts) == 4):
                    # get the individual parts in the filename
                    cur_start_ts = str(parts[0]) + str(parts[1])
                    cur_end_ts = str(parts[2]) + str(parts[3])

                    # apply the filter condition
                    if (not (str(end_date_numstr) < cur_start_ts or str(start_date_numstr) > cur_end_ts)):
                        # note filename1
                        paths_found.append(filename)
                        utils.trace("file_paths_util: get_file_paths_by_datetime_range: found file: {}".format(filename))

    # return
    return paths_found

def get_local_directory_listing(path, filter_func = None, ignore_if_missing = False, skip_exist_check = False):
    fs = s3io_wrapper.S3FSWrapper()
    return fs.get_directory_listing(path, filter_func = filter_func, ignore_if_missing = ignore_if_missing, skip_exist_check = skip_exist_check)

# this method is not robust against complex path creations with dot(.). FIXME. TODO
def create_local_parent_dir(filepath):
    utils.warn_once("create_local_parent_dir: os.makedirs can create the full path. Why do we need this method")

    # initialize fs
    fs = s3io_wrapper.S3FSWrapper()

    # if it is a local file, create the parent directory
    if (filepath.startswith("s3://") == True):
        raise Exception("filepath is in S3:" + filepath)

    # split the path and fetch the parent directory
    parts = list(filter(lambda x: len(x) > 0, filepath.split("/")))
    if (len(parts) > 1):
        dir_path = "/".join(parts[0:-1])
        # prepend the "/" prefix if the path started from root directory
        if (filepath.startswith("/")):
            dir_path = "/" + dir_path

        if (fs.dir_exists(dir_path, None, None) == False):
            if (utils.is_debug()):
                print("Creating local directory:", dir_path)
            os.makedirs(dir_path, exist_ok = True)


