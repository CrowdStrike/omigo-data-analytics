"""utility methods to do logging. this is more of convenience and very likely to be replaced with better implementation"""

import urllib
import urllib.parse
import json
from json.decoder import JSONDecodeError
import os
import math
import mmh3
import time 
from concurrent.futures import ThreadPoolExecutor

# TODO: these caches dont work in multithreaded env. 
MSG_CACHE_MAX_LEN = 10000

def is_debug():
    return str(os.environ.get("OMIGO_DEBUG", "0")) == "1"

def is_trace():
    return str(os.environ.get("OMIGO_TRACE", "0")) == "1"

def get_report_progress():
    return float(os.environ.get("OMIGO_DEBUG_REPORT_PROGRESS_PERC", "0"))

def get_report_progress_min_thresh():
    return float(os.environ.get("OMIGO_DEBUG_REPORT_PROGRESS_MIN_THRESH", "100000"))

def set_report_progress_perc(perc):
    os.environ["OMIGO_DEBUG_REPORT_PROGRESS_PERC"] = str(perc)

def set_report_progress_min_thresh(thresh):
    os.environ["OMIGO_DEBUG_REPORT_PROGRESS_MIN_THRESH"] = str(thresh)

def trace(msg):
    if (is_trace()):
        print("[TRACE]: {}".format(msg))

def trace_once(msg, msg_cache):
    # check if the cache has become too big
    if (len(msg_cache) >= MSG_CACHE_MAX_LEN):
        msg_cache = {}

    # check if msg is already displayed
    if (msg not in msg_cache.keys()):
        print("[TRACE ONCE ONLY]: " + msg)
        msg_cache[msg] = 1

def debug(msg):
    if (is_debug()):
        print("[DEBUG]: {}".format(msg))

def debug_once(msg, msg_cache):
    # check if msg is already displayed
    if (msg not in msg_cache.keys()):
         if (is_debug()):
             print("[DEBUG ONCE ONLY]: {}".format(msg))
         msg_cache[msg] = 1

         # clear the cache if it has become too big
         if (len(msg_cache) >= MSG_CACHE_MAX_LEN):
             msg_cache = {} 
    else:
        trace(msg)
 
def info(msg):
    print("[INFO]: {}".format(msg))

def info_once(msg, msg_cache):
    # check if msg is already displayed
    if (msg not in msg_cache.keys()):
         print("[INFO ONCE ONLY]: {}".format(msg))
         msg_cache[msg] = 1

         # clear the cache if it has become too big
         if (len(msg_cache) >= MSG_CACHE_MAX_LEN):
             msg_cache = {} 
    else:
        trace(msg)
 
def error(msg):
    print("[ERROR]: {}".format(msg))

def error_once(msg, msg_cache):
    # check if msg is already displayed
    if (msg not in msg_cache.keys()):
         print("[ERROR ONCE ONLY]: {}".format(msg))
         msg_cache[msg] = 1

         # clear the cache if it has become too big
         if (len(msg_cache) >= MSG_CACHE_MAX_LEN):
             msg_cache = {} 
    else:
        trace(msg)
 
def enable_debug_mode():
    os.environ["OMIGO_DEBUG"] = "1"

def enable_trace_mode():
    os.environ["OMIGO_TRACE"] = "1"

def disable_debug_mode():
    os.environ["OMIGO_DEBUG"] = "0"

def disable_trace_mode():
    os.environ["OMIGO_TRACE"] = "0"

def warn(msg):
    print("[WARN]: " + msg)

def warn_once(msg, msg_cache):
    # check if msg is already displayed
    if (msg not in msg_cache.keys()):
        print("[WARN ONCE ONLY]: " + msg)
        msg_cache[msg] = 1

        # check if the cache has become too big
        if (len(msg_cache) >= MSG_CACHE_MAX_LEN):
            msg_cache = {}

    else:
        trace(msg)
  
def is_code_todo_warning():
    return str(os.environ.get("OMIGO_CODE_TODO_WARNING", "0")) == "1"

def print_code_todo_warning(msg):
    if (is_code_todo_warning()):
        print("[CODE TODO WARNING]: " + msg)

def url_encode(s):
    if (s is None):
        return "" 

    return urllib.parse.quote_plus(s)

# TODO: this replaces TAB character
def url_decode(s):
    if (s is None):
        return "" 

    return urllib.parse.unquote_plus(s).replace("\t", " ")

# move this to utils
def parse_encoded_json(s):
    if (s is None):
        return {}

    if (s == ""):
        return {}

    try:
        decoded = url_decode(s)
        if (decoded == ""):
            return {}

        return json.loads(decoded)
    except JSONDecodeError:
        print("Error in decoding json")
        return {}

def encode_json_obj(json_obj):
    return utl_encode(json.dumps(json_obj))

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

def is_int_col(xtsv, col):
    try:
        for v in xtsv.col_as_array(col):
            if (str(int(v)) != v):
                return False
    except:
        return False

    return True

def is_float_col(xtsv, col):
    try:
        xtsv.col_as_float_array(col)
    except:
        return False

    return True

def is_pure_float_col(xtsv, col):
    try:
        found = False
        for v in xtsv.col_as_float_array(col):
            if (float(int(v)) != v):
                found = True
        if (found == True):
            return True
        else:
            return False
    except:
        return False

def is_float_with_fraction(xtsv, col):
    if (is_float_col(xtsv, col) == False):
        return False

    found = False
    for v in xtsv.col_as_array(col):
        if ("." in v):
            return True

    return False 

def compute_hash(x, seed = 0):
    return abs(mmh3.hash64(str(x) + str(seed))[0])

class ThreadPoolTask:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

def run_with_thread_pool(tasks, num_par = 4, wait_sec = 10, post_wait_sec = 0):
    # debug
    info("run_with_thread_pool: num tasks: {}, num_par: {}".format(len(tasks), num_par))

    # define results
    results = []

    # check if this is to be run in multi threaded mode or not
    if (num_par == 0):
        info("run_with_thread_pool: running in single threaded mode")

        # iterate
        for task in tasks:
            func = task.func
            args = task.args
            kwargs = task.kwargs
            results.append(func(*args, **kwargs))               

        # return
        return results
    else: 
        # start thread pool
        future_results = []
        with ThreadPoolExecutor(max_workers = num_par) as executor:
            # iterate over tasks and call submit
            for i in range(len(tasks)):
                # call submit
                func = tasks[i].func
                args = tasks[i].args
                kwargs = tasks[i].kwargs
                future_results.append(executor.submit(func, *args, **kwargs))

            # wait for completion
            while True:
                done_count = 0
                for f in future_results:
                    if (f.done() == True):
                        done_count = done_count + 1

                # check if all are done
                if (done_count < len(future_results)):
                    # sleep for some additional time to allow notebook stop method to work
                    info("run_with_thread_pool: futures not completed yet. Status: {} / {}. Sleeping for {} sec".format(done_count, len(future_results), wait_sec))
                    time.sleep(wait_sec)
                else:
                    info("run_with_thread_pool: finished")
                    break

            # combine the results
            for f in future_results:
                results.append(f.result())

            # wait for post_wait_sec for mitigating eventual consistency
            if (post_wait_sec > 0):
                info("run_with_thread_pool: sleeping for post_wait_sec: {}".format(post_wait_sec))
                time.sleep(post_wait_sec)

            # return
            return results

