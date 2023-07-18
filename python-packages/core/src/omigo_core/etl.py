"""EtlDateTimePathFormat class"""
from omigo_core import tsv, utils, tsvutils, funclib, file_paths_util
from dateutil import parser
import datetime
import random

# extension functions for ETL related stuff

# expects path in the form of s3://x1/x2/dt=yyyymmdd/abc1-abc2-yymmdd-HHMMSS-yymmdd-HHMMSS.tsv.gz
# returns
# dir_prefix: s3://x1/x2
# date_part: yyyymmdd
# base_prefix: abc1-abc2
# start_date: yymmdd
# start_time: HHMMSS
# end_date: yymmdd
# end_time: HHMMSS
# extension: tsv.gz
# self.start_date_time: yyyy-MM-ddTHH:MM:SS
# self.end_date_time: yyyy-MM-ddTHH:MM:SS

class EtlDateTimePathFormat:
    # fields
    dir_prefix = None
    date_part = None

    base_prefix = None
    start_date = None
    start_time = None
    end_date = None
    end_time = None
    extension = None

    # synthetic forms
    start_date_time = None
    end_date_time = None

    def __init__(self, path):
        utils.print_code_todo_warning("Refactor this using match class, and handle multiple templates")

        parts = path.split("/")
        if (len(parts) < 2 or parts[-2].startswith("dt=") == False):
            raise Exception("Invalid format for path:", path)

        # extract parts
        self.date_part = parts[-2][len("dt="):]
        self.dir_prefix = "/".join(parts[0:-2])
        base_filename = parts[-1]

        # it is okay to fail on exception as we are assuming some basic file structure
        if (base_filename.endswith(".tsv.gz")):
            self.extension = "tsv.gz"
        elif (base_filename.endswith(".tsv.zip")):
            self.extension = "tsv.zip"
        elif (base_filename.endswith(".tsv")):
            self.extension = "tsv"
        else:
            raise Exception("Invalid file. Extension not supported:", path)

        # get file_prefix
        file_prefix = base_filename[0:-(len(self.extension)+1)]

        # split the file_prefix into the start and end datetime
        filename_parts = file_prefix.split("-")
        if (len(filename_parts) < 5):
            raise Exception("Invalid file format:", path, base_filename)

        # prefix
        self.end_time = str(filename_parts[-1])
        self.end_date = str(filename_parts[-2])
        self.start_time = str(filename_parts[-3])
        self.start_date = str(filename_parts[-4])
        self.base_prefix = "-".join(filename_parts[0:-4])

        # synthetic forms
        utils.print_code_todo_warning("the hour 240000 was a mistake. Change it to different end time")

        # original values
        self.start_date_time = "{}-{}-{}T{}:{}:{}".format(self.start_date[0:4], self.start_date[4:6], self.start_date[6:8], self.start_time[0:2], self.start_time[2:4], self.start_time[4:6])
        self.end_date_time = "{}-{}-{}T{}:{}:{}".format(self.end_date[0:4], self.end_date[4:6], self.end_date[6:8], self.end_time[0:2], self.end_time[2:4], self.end_time[4:6])

    def get_correct_end_datetime(self):
        if (self.end_time == "240000"):
            effective_end_dt = parser.parse(self.end_date) + datetime.timedelta(days = 1)
            return effective_end_dt.strftime("%Y-%m-%dT00:00:00")
        else:
             return self.end_date_time

    def to_string(self):
        return "dir_prefix: {}, dt: {}, base_prefix: {}, start_date: {}, start_time: {}, end_date: {}, end_time: {}, extension: {}, start_date_time: {}, end_date_time: {}".format(
            self.dir_prefix, self.date_part, self.base_prefix, self.start_date, self.start_time, self.end_date, self.end_time, self.extension,
            self.start_date_time, self.end_date_time)

def get_matching_etl_date_time_path(path, new_base_path, new_prefix, new_extension = None):
     # parse the old path
     er = EtlDateTimePathFormat(path)
     effective_extension = new_extension if (new_extension is not None) else er.extension

     # construct new path
     new_path = "{}/dt={}/{}-{}-{}-{}-{}.{}".format(new_base_path, er.date_part, new_prefix, er.start_date, er.start_time, er.end_date, er.end_time, effective_extension)
     return new_path

# returns date needed in function parameters
def get_etl_date_str_from_ts(ts):
    dtime = funclib.utctimestamp_to_datetime(ts)
    return dtime.strftime("%Y-%m-%d")

# returns date needed in function parameters
def get_etl_datetime_str_from_ts(ts):
    dtime = funclib.utctimestamp_to_datetime(ts)
    return dtime.strftime("%Y-%m-%dT%H:%M:%S")

# returns date needed in filenames
def get_etl_file_date_str_from_ts(ts):
    dtime = funclib.utctimestamp_to_datetime(ts)
    return dtime.strftime("%Y%m%d")

# returns date needed in filenames
def get_etl_file_datetime_str_from_ts(ts):
    dtime = funclib.utctimestamp_to_datetime(ts)
    return dtime.strftime("%Y%m%d-%H%M%S")

# this method generates the basename minus the extension
# prefix-startyyyymmdd-HHMMSS-endyyyymmdd-HHMMSS
def get_etl_file_base_name_by_ts(prefix, start_ts, end_ts):
    # convert to datetime
    start_datetime = funclib.utctimestamp_to_datetime(start_ts)
    end_datetime = funclib.utctimestamp_to_datetime(end_ts)

    # format
    start_datetime_str = start_datetime.strftime("%Y%m%d-%H%M%S")
    end_datetime_str = end_datetime.strftime("%Y%m%d-%H%M%S")

    # return
    return "{}-{}-{}".format(prefix, start_datetime_str, end_datetime_str)

def scan_by_datetime_range(path, start_date_str, end_date_str, prefix, filter_transform_func = None, cols = None, transform_func = None, spillover_window = 1, num_par = 5,
    wait_sec = 5, timeout_seconds = 600, def_val_map = {}, sampling_rate = None, s3_region = None, aws_profile = None):

    # debug
    utils.info("scan_by_datetime_range: path: {}, start_date_str: {}, end_date_str: {}, spillover_window: {}, def_val_map: {}, sampling_rate: {}".format(
        path, start_date_str, end_date_str, spillover_window, def_val_map, sampling_rate))

    # read filepaths by scanning. this involves listing all the files, and then matching the condititions
    filepaths = get_file_paths_by_datetime_range(path,  start_date_str, end_date_str, prefix, spillover_window = spillover_window, num_par = num_par, sampling_rate = sampling_rate,
        s3_region = s3_region, aws_profile = aws_profile)

    # debug
    utils.info("scan_by_datetime_range: number of paths to read: {}, num_par: {}, timeout_seconds: {}".format(len(filepaths), num_par, timeout_seconds))

    # do some checks on the headers in the filepaths

    # debug
    utils.debug("tsvutils: scan_by_datetime_range: number of files to read: {}".format(len(filepaths)))

    # read all the files in the filepath applying the filter function
    tasks = []

    # iterate over filepaths and submit
    for filepath in filepaths:
        tasks.append(utils.ThreadPoolTask(tsvutils.read_with_filter_transform, filepath, filter_transform_func = filter_transform_func, cols = cols, transform_func = transform_func,
            s3_region = s3_region, aws_profile = aws_profile))

    # execute and get results
    tsv_list = utils.run_with_thread_pool(tasks, num_par = num_par, wait_sec = wait_sec)

    # combine all together
    tsv_combined = tsv.merge(tsv_list, def_val_map = def_val_map)
    utils.info("scan_by_datetime_range: Number of records: {}".format(tsv_combined.num_rows()))

    # return
    return tsv_combined

# this method is needed so that users dont have to interact with file_paths_util
def get_file_paths_by_datetime_range(path, start_date_str, end_date_str, prefix, spillover_window = 1, sampling_rate = None, num_par = 10, wait_sec = 1, s3_region = None, aws_profile = None):
    # get all the filepaths
    filepaths = file_paths_util.get_file_paths_by_datetime_range(path,  start_date_str, end_date_str, prefix, spillover_window = spillover_window, num_par = num_par, wait_sec = wait_sec,
        s3_region = s3_region, aws_profile = aws_profile)

    # check for sampling rate
    if (sampling_rate is not None):
        # validation
        if (sampling_rate < 0 or sampling_rate > 1):
            raise Exception("sampling_rate is not valid: {}".format(sampling_rate))

        # determine the number of samples to take
        sample_n = int(len(filepaths) * sampling_rate)
        random.shuffle(filepaths)
        filepaths = sorted(filepaths[0:sample_n])
    else:
        filepaths = sorted(filepaths)

    # return
    return filepaths

