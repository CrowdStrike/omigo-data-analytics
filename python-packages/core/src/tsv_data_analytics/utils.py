"""utility methods to do logging. this is more of convenience and very likely to be replaced with better implementation"""

import urllib
import urllib.parse
import json
from json.decoder import JSONDecodeError
import os
import math

def is_debug():
    return str(os.environ.get("TSV_DATA_ANALYTICS_DEBUG", "0")) == "1"

def is_trace():
    return str(os.environ.get("TSV_DATA_ANALYTICS_TRACE", "0")) == "1"

def get_report_progress():
    return float(os.environ.get("TSV_DATA_ANALYTICS_DEBUG_REPORT_PROGRESS", "0"))

def get_report_progress_min_thresh():
    return float(os.environ.get("TSV_DATA_ANALYTICS_DEBUG_REPORT_PROGRESS_MIN_THRESH", "100000"))

def trace(msg):
    if (is_trace()):
        print("[TRACE]: {}".format(msg))

def debug(msg):
    if (is_debug()):
        print("[DEBUG]: {}".format(msg))

def info(msg):
    print("[INFO]: {}".format(msg))

def enable_debug_mode():
    os.environ["TSV_DATA_ANALYTICS_DEBUG"] = "1"

def enable_trace_mode():
    os.environ["TSV_DATA_ANALYTICS_TRACE"] = "1"

def disable_debug_mode():
    os.environ["TSV_DATA_ANALYTICS_DEBUG"] = "0"

def disable_trace_mode():
    os.environ["TSV_DATA_ANALYTICS_TRACE"] = "0"

def warn(msg):
    print("[WARN]: " + msg)

def is_code_todo_warning():
    return str(os.environ.get("TSV_DATA_ANALYTICS_CODE_TODO_WARNING", "0")) == "1"

def print_code_todo_warning(msg):
    if (is_code_todo_warning()):
        print("[CODE TODO WARNING]: " + msg)

def url_encode(s):
    return urllib.parse.quote_plus(s)

def url_decode(s):
    return urllib.parse.unquote_plus(s)

# move this to utils
def parse_encoded_json(s):
    if (s == None):
        return {}

    if (s == ""):
        return {}

    try:
        decoded = urllib.parse.unquote_plus(s)
        if (decoded == ""):
            return {}

        return json.loads(decoded)
    except JSONDecodeError:
        print("Error in decoding json")
        return {}

def encode_json_obj(json_obj):
    return urllib.parse.quote_plus(json.dumps(json_obj))

# TODO: make it more robust. The object key doesnt have / prefix or suffix
def split_s3_path(path):
    part1 = path[5:]
    index = part1.index("/")
    bucket_name = part1[0:index]

    # boundary conditions
    if (index < len(part1) - 1):
        object_key = part1[index+1:]
    else:
        object_key = ""

    # remove trailing suffix
    if(object_key.endswith("/")):
        object_key = object_key[0:-1]

    return bucket_name, object_key

def get_counts_map(xs):
    mp = {}
    for x in xs:
        if (x not in mp):
            mp[x] = 0
        mp[x] = mp[x] + 1

    return mp 

def report_progress(msg, inherit_message, counter, total):
    report_progress = get_report_progress()
    report_progress_min_threshold = get_report_progress_min_thresh()
    msg2 = inherit_message + ": " + msg if (len(inherit_message) > 0) else msg
    if (is_debug() and report_progress > 0 and total >= report_progress_min_threshold):
        progress_size = int(report_progress * total)
        if (progress_size > 0 and counter % progress_size == 0):
            progress_perc = int(math.ceil(100 * counter / total))
            debug("{}: {}% ({} / {})".format(msg2, progress_perc, counter, total))

def merge_arrays(arr_list):
    result = []
    for arr in arr_list:
        for v in arr:
            result.append(v)

    return result


def is_array_of_string_values(col_or_cols):
    if (isinstance(col_or_cols, str)):
        return False

    is_array = False
    for c in col_or_cols:
        if (len(c) > 1):
            is_array = True
            break
    return is_array

