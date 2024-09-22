"""library of function to be used in general purpose data processing for TSV class"""

import statistics
import numpy as np
from dateutil import parser
import datetime
from omigo_core import utils
from omigo_core import udfs, timefuncs

# TODO: mkstr variants needs to use *args

def parse_image_file_base_name(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.parse_file_base_name(*args, **kwargs)

def get_len(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.get_len(*args, **kwargs)

def get_non_empty_len(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.get_non_empty_len(*args, **kwargs)

def uniq_len(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.uniq_len(*args, **kwargs)

def uniq_mkstr(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.uniq_mkstr(*args, **kwargs)

def split_merge_uniq_mkstr(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.split_merge_uniq_mkstr(*args, **kwargs)

def mean(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.mean(*args, **kwargs)

def std_dev(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.std_dev(*args, **kwargs)

def mkstr(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.mkstr(*args, **kwargs)

def sorted_mkstr(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.sorted_mkstr(*args, **kwargs)

def mkstr4f(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.mkstr4f(*args, **kwargs)

def minint(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.minint(*args, **kwargs)

def maxint(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.maxint(*args, **kwargs)

def minfloat(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.minfloat(*args, **kwargs)

def maxfloat(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.maxfloat(*args, **kwargs)

def minstr(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.minstr(*args, **kwargs)

def maxstr(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.maxstr(*args, **kwargs)

def minint_failsafe(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.minint_failsafe(*args, **kwargs)

def maxint_failsafe(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.maxint_failsafe(*args, **kwargs)

def minstr_failsafe(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.minstr_failsafe(*args, **kwargs)
            
def maxstr_failsafe(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.maxstr_failsafe(*args, **kwargs)

def sumint(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.sumint(*args, **kwargs)

def sumfloat(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.sumfloat(*args, **kwargs)

# TODO: The semantics are not clear
def uniq_count(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.uniq_count(*args, **kwargs)

def merge_uniq(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.merge_uniq(*args, **kwargs)

def select_first(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.select_first(*args, **kwargs)

def select_first_non_empty(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.select_first_non_empty(*args, **kwargs)

def select_max_int(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.select_max_int(*args, **kwargs)

def str_arr_to_float(xs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return [float(x) for x in xs]

def quantile(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.quantile(*args, **kwargs)

def quantile4(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.quantile4(*args, **kwargs)

def quantile10(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.quantile10(*args, **kwargs)

def quantile40(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.quantile40(*args, **kwargs)

def max_str(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.max_str(*args, **kwargs)

def min_str(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.min_str(*args, **kwargs)

def to2digit(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.to2digit(*args, **kwargs)

def to4digit(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.to2digit(*args, **kwargs)

def to6digit(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.to2digit(*args, **kwargs)

def convert_prob_to_binary(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use udfs package")
    return udfs.convert_prob_to_binary(*args, **kwargs)

def get_str_map_with_keys(mp, keys, fail_on_missing = True):
    mp2 = {}
    for k in keys:
        if (k in mp.keys()):
            mp2[k] = str(mp[k])
        else:
            if (fail_on_missing):
                raise Exception("Missing key: {}, avaialble keys: {}".format(k, str(mp.keys())))

    return mp2

def get_str_map_without_keys(mp, excluded_keys):
    mp2 = {}
    for k in mp.keys():
        if (k not in excluded_keys):
            mp2[k] = str(mp[k])

    return mp2

# def if_else_non_empty_str(x, v1, v2):
def if_else_non_empty_str(*args):
    return udfs.if_else_non_empty_str(*args)

# def if_else_str(xinput, xval, v1, v2):
def if_else_str(*args):
    return udfs.if_else_str(*args)

# def if_else_int(xinput, xval, v1, v2):
def if_else_int(*args):
    return udfs.if_else_int(*args)

# def if_else_non_zero_int(x, v1, v2):
def if_else_non_zero_int(*args):
    return udfs.if_else_non_zero_int(*args)

# TODO: this is bad implementation. The Win32 timestamp format needs proper handling
def win32_timestamp_to_utctimestamp(x):
    return int(str(x)[0:-8]) + 339576461

def get_time_diffs(vs):
    # sort the input
    vs = sorted(list([datetime_to_utctimestamp_sec(t) for t in vs]))

    # boundary condition
    if (len(vs) <= 1):
        return ""

    # get parirs
    pairs = list(zip(vs[0:len(vs)-1], vs[1:]))

    # result
    result = []
    for pair in pairs:
        v1, v2 = pair
        diff = int(v2 - v1)
        if (diff < 60):
            result.append("{}s".format(diff))
        elif (diff < 60 * 60):
            result.append("{}m".format(int(diff / 60)))
        elif (diff < 24 * 60 * 60):
            result.append("{}h".format(int(diff / (60 * 60))))
        elif (diff < 30 * 24 * 60 * 60):
            result.append("{}d".format(int(diff / (24 * 60 * 60))))
        else:
            result.append("{}:s".format(diff))

    # return
    return ",".join(result)

def simple_map_to_url_encoded_col_names(cols, url_encoded_cols = None):
    # create result
    results = []
    if (url_encoded_cols is not None):
        # iterate
        for c in cols:
            if (c in url_encoded_cols):
                results.append("{}:url_encoded".format(c))
            else:
                results.append(c)
    else:
        results = cols

    # return
    return results

def map_to_url_encoded_col_names(cols, prefix = None, url_encoded_cols = None):
    results = []

    # iterate
    for c in cols:
        col = c
        # check if there is prefix 
        if (col.find(":") != -1):
            col = col.split(":")[-1]

        # assign
        result = c

        # handle url_encoded suffix
        if (url_encoded_cols is not None and col in url_encoded_cols):
            result = "{}:url_encoded".format(c)

        # handle prefix
        if (prefix is not None):
            result = "{}:{}".format(prefix, result)

        # append
        results.append(result)

    # return
    return results

def get_display_relative_time_str(v):
    # convert to int
    v = int(v)

    # compute units
    days = v // 86400
    hours = (v - (days * 86400)) // 3600
    minutes = (v - (days * 86400 + hours * 3600)) // 60
    seconds = v - (days * 86400 + hours * 3600 + minutes * 60)
            
    # compute display string
    results = []
    count = 0
    max_display_values = 2
    if (days > 0 and count < max_display_values):
        results.append("{}d".format(days))
        count = count + 1
    if (hours > 0 and count < max_display_values):
        results.append("{}h".format(hours))
        count = count + 1
    if (minutes > 0 and count < max_display_values):
        results.append("{}m".format(minutes))
        count = count + 1
    if (seconds > 0 and count < max_display_values):
        results.append("{}s".format(seconds))
        count = count + 1

    # get string
    result = " ".join(results)

    # return
    return result

def datetime_to_utctimestamp_millis(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.datetime_to_utctimestamp_millis(*args, **kwargs)

def datetime_to_utctimestamp(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.datetime_to_utctimestamp(*args, **kwargs)

def datetime_to_utctimestamp_sec(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.datetime_to_utctimestamp_sec(*args, **kwargs)

def utctimestamp_to_datetime(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.utctimestamp_to_datetime(*args, **kwargs)

def utctimestamp_millis_to_datetime(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.utctimestamp_millis_to_datetime(*args, **kwargs)

def utctimestamp_to_datetime_str(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.utctimestamp_to_datetime_str(*args, **kwargs)

def utctimestamp_millis_to_datetime_str(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.utctimestamp_millis_to_datetime_str(*args, **kwargs)

def datetime_to_timestamp(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.datetime_to_timestamp(*args, **kwargs)

def get_utctimestamp_sec(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.get_utctimestamp_sec(*args, **kwargs)

def get_utctimestamp_millis(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return timefuncs.get_utctimestamp_millis(*args, **kwargs)

def datestr_to_datetime(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use timefuncs package")
    return datestr_to_datetime(*args, **kwargs)

