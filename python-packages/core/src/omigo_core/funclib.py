"""library of function to be used in general purpose data processing for TSV class"""

import statistics
import numpy as np
from dateutil import parser
import datetime

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
    return statistics.mean(vs)

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
    return "{:.2f}".format(x)

def to4digit(x):
    return "{:.4f}".format(x)

def to6digit(x):
    return "{:.6f}".format(x)

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
    elif (len(str(x)) == 10 and str(x).isnumeric() == True):
        # this looks like a numeric timestamp
        return int(x)
    elif (len(str(x)) == 13 and str(x).isnumeric() == True):
        # this looks like numeric timestamp in millis
        return int(int(x) / 1000)
    else:
        raise Exception("Unknown date format. Problem with UTC: '{}'".format(x))

# TODO: Converts seconds format only
def utctimestamp_to_datetime_str(x):
    return utctimestamp_to_datetime(x).isoformat()

# TODO: Converts seconds format only
def utctimestamp_to_datetime(x):
    # take it as int
    x = int(x)
    if (len(str(x)) == 10):
        return datetime.datetime.utcfromtimestamp(x).replace(tzinfo = datetime.timezone.utc)
    elif (len(str(x)) == 13):
        return datetime.datetime.utcfromtimestamp(int(x)//1000).replace(tzinfo = datetime.timezone.utc)
    else:
        raise Exception("Unknown timestamp format: {}".format(x))

def datetime_to_timestamp(x):
    raise Exception("Please use datetime_to_utctimestamp")

def get_utctimestamp_sec():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())

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
