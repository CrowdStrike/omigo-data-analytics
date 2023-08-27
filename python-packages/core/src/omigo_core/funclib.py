"""library of function to be used in general purpose data processing for TSV class"""

import statistics
import numpy as np
from dateutil import parser
import datetime
from omigo_core import utils

# TODO: mkstr variantgs needs to use *args

def parse_image_file_base_name(x):
    if (len(x) <= 1):
        return x

    index = -1
    if ("\\" in x):
        index = x.rindex("\\")
    elif ("/" in x):
        index = x.rindex("/")
    return str(x[index+1:])

def get_len(vs):
    return str(len(vs))

def get_non_empty_len(vs):
    vs = list(filter(lambda t: len(t.strip()) > 0, vs))
    return str(len(vs))

def uniq_len(vs):
    vs2 = set()
    for t in vs:
        for k in str(t).split(","):
            if (len(k.strip()) > 0):
                vs2.add(str(k))
    return str(len(vs2))

def uniq_mkstr(vs):
    vs2 = set()
    for t in vs:
        for k in str(t).split(","):
            if (len(k.strip()) > 0):
                vs2.add(str(k))
    return ",".join(sorted([str(x) for x in vs2]))

def split_merge_uniq_mkstr(vs):
    vs2 = []
    for v in vs:
        vs2 = vs2 + v.split(",")
    vs2 = list(set(filter(lambda t: len(t.strip()) > 0, vs2)))
    return ",".join(sorted([str(x) for x in vs2]))

def mean(vs):
    vs = list([float(v) for v in vs])
    return statistics.mean(vs)

def std_dev(vs):
    vs = list([float(v) for v in vs])
    return statistics.stdev(vs)

def mkstr(vs):
    vs2 = list(filter(lambda t: len(t.strip()) > 0, [str(x) for x in vs]))
    return ",".join(vs2)

def sorted_mkstr(vs):
    vs2 = sorted(list(filter(lambda t: len(t.strip()) > 0, [str(x) for x in vs])))
    return ",".join(vs2)

def mkstr4f(vs):
    vs2 = list(["{:4f}".format(float(x)) for x in vs])
    return ",".join(vs2)

def minint(vs):
    if (vs is None or len(vs) == 0):
        raise Exception("minint: empty vs")

    min_value = str(vs[0])
    for v in vs[1:]:
        if (int(float(v)) < int(float(min_value))):
            min_value = str(v)

    return str(min_value)

def maxint(vs):
    if (vs is None or len(vs) == 0):
        raise Exception("maxint: empty vs")

    max_value = str(vs[0])
    for v in vs[1:]:
        if (int(float(v)) > int(float(max_value))):
            max_value = str(v)

    return str(max_value)

def minfloat(vs):
    if (vs is None or len(vs) == 0):
        raise Exception("minfloat: empty vs")

    min_value = str(vs[0])
    for v in vs[1:]:
        if (float(v) < float(min_value)):
            min_value = str(v)

    return str(min_value)

def maxfloat(vs):
    if (vs is None or len(vs) == 0):
        raise Exception("maxfloat: empty vs")

    max_value = str(vs[0])
    for v in vs[1:]:
        if (float(v) > float(max_value)):
            max_value = str(v)

    return str(max_value)

def minstr(vs):
    if (vs is None or len(vs) == 0):
        raise Exception("minstr: empty vs")

    min_value = str(vs[0])
    for v in vs[1:]:
        if (str(v) < min_value):
            min_value = str(v)

    return str(min_value)

def maxstr(vs):
    if (vs is None or len(vs) == 0):
        raise Exception("maxstr: empty vs")

    max_value = str(vs[0])
    for v in vs[1:]:
        if (str(v) > max_value):
            max_value = str(v)

    return str(max_value)

def minint_failsafe(vs):
    if (vs is None or len(vs) == 0):
        return "" 
    else:
        vs = list(filter(lambda t: t != "", vs))
        if (len(vs) == 0):
            return "" 
        else:
            return minint(vs) 

def maxint_failsafe(vs):
    if (vs is None or len(vs) == 0):
        return "" 
    else:
        vs = list(filter(lambda t: t != "", vs))
        if (len(vs) == 0):
            return "" 
        else:
            return maxint(vs) 

def minstr_failsafe(vs):
    if (vs is None or len(vs) == 0):
        return "" 
    else:
        vs = list(filter(lambda t: t != "", vs))
        if (len(vs) == 0):
            return "" 
        else:
            return minstr(vs) 
            
def maxstr_failsafe(vs):
    if (vs is None or len(vs) == 0):
        return "" 
    else:
        vs = list(filter(lambda t: t != "", vs))
        if (len(vs) == 0):
            return "" 
        else:
            return maxstr(vs)

def sumint(vs):
    if (len(vs) == 0):
        return 0
    else:
        return sum([int(float(t)) for t in vs])

def sumfloat(vs):
    if (len(vs) == 0):
        return 0.0
    else:
        return sum([float(t) for t in vs])

def uniq_count(vs):
    return len(set(vs))

def merge_uniq(vs):
    result = []
    for v in vs:
        for v2 in v.split(","):
            result.append(v2)

    # return
    return ",".join(sorted(list(set(result))))

def select_first(x, y):
    return x

def select_max_int(x, y):
    return int(max(int(x), int(y)))

def str_arr_to_float(xs):
    return [float(x) for x in xs]

def quantile(xs, start = 0, end = 1, by = 0.25, precision = 4):
    if (start > end):
        raise Exception("Start: {} > End: {}".format(start, end))

    qarr = []
    cur = start
    while (cur < end):
        qarr.append(cur)
        cur = cur + by

    format_str = "{:." + str(precision) + "f}"
    quan = np.quantile(str_arr_to_float(xs), qarr)
    return ",".join(format_str.format(x) for x in quan)

def quantile4(xs):
    return quantile(xs)

def quantile10(xs):
    return quantile(xs, by=1/10)

def quantile40(xs):
    return quantile(xs, by=1/40)

def max_str(xs):
    utils.warn_once("max_str is deprecated. Use maxstr")
    xs = sorted(xs)
    return xs[-1]

def min_str(xs):
    utils.warn_once("min_str is deprecated. Use maxstr")
    xs = sorted(xs)
    return xs[0]

def to2digit(x):
    return "{:.2f}".format(float(x))

def to4digit(x):
    return "{:.4f}".format(float(x))

def to6digit(x):
    return "{:.6f}".format(float(x))

def convert_prob_to_binary(x, split=0.5):
    if (x >= split):
        return 1
    else:
        return 0

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

def datetime_to_utctimestamp_millis(x):
    # convert this to string first for failsafe
    x = str(x)

    # 1681202675933
    if (len(str(x)) == 13 and str(x).isnumeric() == True):
        # this looks like numeric timestamp in millis
        return int(x)

    # 1681202675.933
    if (len(str(x)) == 14 and str(x).find(".") == 10 and str(x).isnumeric() == True):
        # this looks like numeric timestamp in millis
        return int(x)

    # 2023-04-11T08:44:35.933Z
    if (len(x) == 24 and x[19] == "." and x.endswith("Z")):
        return int(float(parser.parse(x).timestamp() * 1000))

    # 2023-04-15T15:05:16.175000Z
    if (len(x) == 27 and x[19] == "." and x.endswith("Z")):
        return int(float(parser.parse(x).timestamp() * 1000))

    # 2023-04-11T08:44:35.933+00:00
    if (len(x) == 29 and x[-6] == "+"):
        return int(float(parser.parse(x).timestamp() * 1000))

    # 2023-04-18T18:47:45 or 2023-04-18 18:47:45
    if (len(x) == 19 and (x[10] == "T" or x[10] == " ")):
        return int(float(parser.parse(x + "+00:00").timestamp() * 1000))

    # this seems to be a timestamp with second precision.
    return int(datetime_to_utctimestamp(x) * 1000)

# TODO. better naming
def datetime_to_utctimestamp(x):
    # convert this to string first for failsafe
    x = str(x)

    # 2022-05-20T05:00:00+00:00
    if (x.endswith("UTC") or x.endswith("GMT") or x.endswith("Z") or x.endswith("+00:00")):
        return int(parser.parse(x).timestamp())
    elif (len(x) == 10 and x.find("-") != -1):
        # 2021-11-01
        x = x + "T00:00:00Z"
        return int(parser.parse(x).timestamp())
    elif (len(x) == 19):
        # 2021-11-01T00:00:00
        x = x + "Z"
        if (x[10] == " "):
            x = x.replace(" ", "T")
        return int(parser.parse(x).timestamp())
    elif (len(x) == 26):
        # 2021-11-01T00:00:00.000000
        x = x + "Z"
        return int(parser.parse(x).timestamp())
    elif (len(x) == 27 and x[19] == "." and x.endswith("Z")):
        # 2023-04-15T15:05:16.175000Z
        return int(parser.parse(x).timestamp())
    elif (len(x) == 29 and x[-6] == "+"):
        # 2023-04-11T08:44:35.933+00:00
        return int(parser.parse(x).timestamp())
    elif (len(x) == 10 and str(x).isnumeric() == True):
        # this looks like a numeric timestamp
        return int(x)
    elif (len(x) == 13 and str(x).isnumeric() == True):
        # this looks like numeric timestamp in millis
        return int(int(x) / 1000)
    elif (len(x) == 19 and (x[10] == "T" or x[10] == " ")):
        # 2023-04-18T18:47:45 or 2023-04-18 18:47:45
        return int(float(parser.parse(x + "+00:00").timestamp() * 1000))
    else:
        raise Exception("Unknown date format. Problem with UTC: '{}'".format(x))

# TODO: Converts seconds format only
def utctimestamp_to_datetime(x):
    # use the string form
    x = str(x)
    if (len(x) == 10 and x.isnumeric() == True):
        return datetime.datetime.utcfromtimestamp(int(x)).replace(tzinfo = datetime.timezone.utc)
    elif (len(x) == 13 and x.isnumeric() == True):
        return datetime.datetime.utcfromtimestamp(int(x)/1000).replace(tzinfo = datetime.timezone.utc)
    elif (len(x) > 10 and x.find(".") == 10 and utils.is_float(x)): 
        return datetime.datetime.utcfromtimestamp(float(x)).replace(tzinfo = datetime.timezone.utc)
    else:
        raise Exception("Unknown timestamp format: {}".format(x))

# TODO: Converts seconds format only
def utctimestamp_millis_to_datetime(x):
    return utctimestamp_to_datetime(x)

# TODO: Converts seconds format only
# Its utc so removed the last timezone
def utctimestamp_to_datetime_str(x):
    return utctimestamp_to_datetime(x).isoformat()[0:19]

# Its utc so removed the last timezone
def utctimestamp_millis_to_datetime_str(x):
    return utctimestamp_to_datetime(x).isoformat()[0:23]

def datetime_to_timestamp(x):
    raise Exception("Please use datetime_to_utctimestamp")

def get_utctimestamp_sec():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())

def get_utctimestamp_millis():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

def datestr_to_datetime(x):
    return utctimestamp_to_datetime(datetime_to_utctimestamp(x))

def select_first_non_empty(*args, **kwargs):
    # variable name
    xs = args

    # boundary condition
    if (xs is None or len(xs) == 0):
        return ""

    # check if it is list or tuple
    if (isinstance(xs[0], list)):
        xs = xs[0]

    # boundary conditions
    for x in xs:
        if (x is not None and x != ""):
            return str(x)

    # return default
    return ""

# def if_else_non_empty_str(x, v1, v2):
def if_else_non_empty_str(*args):
    # variable name
    xs = args

    # boundary condition
    if (xs is None or len(xs) == 0):
        raise Exception("if_else_non_empty_str: invalid input") 

    # check if it is list or tuple
    if (isinstance(xs[0], list)):
        xs = xs[0]

    # validation
    if (len(xs) != 3):
        raise Exception("if_else_non_empty_str: invalid input: {}".format(xs))

    # assign variables
    x = xs[0]
    v1 = xs[1]
    v2 = xs[2]

    if (x is not None and str(x) != ""):
        return str(v1)
    else:
        return str(v2)

# def if_else_str(xinput, xval, v1, v2):
def if_else_str(*args):
    # variable name
    xs = args

    # boundary condition
    if (xs is None or len(xs) == 0):
        raise Exception("if_else_non_empty_str: invalid input") 

    # check if it is list or tuple
    if (isinstance(xs[0], list)):
        xs = xs[0]

    # validation
    if (len(xs) != 4):
        raise Exception("if_else_str: invalid input: {}".format(xs))

    # assign variables
    xinput = xs[0]
    xval = xs[1]
    v1 = xs[2]
    v2 = xs[3]

    # apply function
    if (xinput is not None and str(xinput) == str(xval)):
        return str(v1)
    else:
        return str(v2)

# def if_else_int(xinput, xval, v1, v2):
def if_else_int(*args):
    # variable name
    xs = args

    # boundary condition
    if (xs is None or len(xs) == 0):
        raise Exception("if_else_int: invalid input") 

    # check if it is list or tuple
    if (isinstance(xs[0], list)):
        xs = xs[0]

    # validation
    if (len(xs) != 4):
        raise Exception("if_else_int: invalid input: {}".format(xs))

    # assign variables
    xinput = xs[0]
    xval = xs[1]
    v1 = xs[2]
    v2 = xs[3]

    # apply function
    if (xinput is not None and int(xinput) == int(xval)):
        return v1
    else:
        return v2

# def if_else_non_zero_int(x, v1, v2):
def if_else_non_zero_int(*args):
    # variable name
    xs = args

    # boundary condition
    if (xs is None or len(xs) == 0):
        raise Exception("if_else_non_zero_int: invalid input") 

    # check if it is list or tuple
    if (isinstance(xs[0], list)):
        xs = xs[0]

    # validation
    if (len(xs) != 3):
        raise Exception("if_else_int: invalid input: {}".format(xs))

    # assign variables
    x = xs[0]
    v1 = xs[1]
    v2 = xs[2]

    # apply function
    if (x is None or int(x) != 0):
        return v1
    else:
        return v2

# TODO: this is bad implementation. The Win32 timestamp format needs proper handling
def win32_timestamp_to_utctimestamp(x):
    return int(str(x)[0:-8]) + 339576461

def get_time_diffs(vs):
    # sort the input
    vs = sorted(list([datetime_to_utctimestamp(t) for t in vs]))

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

