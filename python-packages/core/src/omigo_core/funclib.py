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
    
def uniq_len(vs):
    return str(len(set(vs)))

def uniq_mkstr(vs):
    vs2 = set()
    for t in vs:
        for k in str(t).split(","):
            if (len(k) > 0):
                vs2.add(str(k))
    return ",".join([str(x) for x in vs2])

def mean(vs):
    return statistics.mean(vs)

def mkstr(vs):
    vs2 = list([str(x) for x in vs])
    return ",".join(vs2)

def mkstr4f(vs):
    vs2 = list(["{:4f}".format(float(x)) for x in vs])
    return ",".join(vs2)

def minstr(vs):
    return sorted(vs)[0]

def maxstr(vs):
    return sorted(vs)[-1]

def uniq_count(vs):
    return len(set(vs))

def merge_uniq(vs):
    result = []
    for v in vs:
        for v2 in v.split(","):
            result.append(v2)
            
    return ",".join(sorted(list(set(result))))

def select_first(x, y):
    return x

def select_max_int(x, y):
    return int(max(int(x), int(y)))

def str_arr_to_float(xs):
    return [float(x) for x in xs]

def quantile(xs, start = 0, end = 1, by = 0.25, precision = 4):
    if (start > end):
        raise Exception("Start > End", start, end)
        
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
    xs = sorted(xs)
    return xs[-1]

def min_str(xs):
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

# TODO
def datetime_to_utctimestamp(x):
    if (x.endswith("UTC") or x.endswith("GMT") or x.endswith("Z") or x.endswith("+00:00")):
        return int(parser.parse(x).timestamp())
    elif (len(x) == 10):
        # 2021-11-01
        x = x + "T00:00:00Z"
        return int(parser.parse(x).timestamp())
    elif (len(x) == 19):
        # 2021-11-01T00:00:00
        x = x + "Z"
        return int(parser.parse(x).timestamp())
    elif (len(x) == 26):
        # 2021-11-01T00:00:00.000000
        x = x + "Z"
        return int(parser.parse(x).timestamp())
    else:
        raise Exception("Unknown date format. Problem with UTC: {}".format(x))

# TODO: Converts seconds format only
def utctimestamp_to_datetime_str(x):
    if (len(str(x)) == 10):
        return datetime.datetime.utcfromtimestamp(x).replace(tzinfo = datetime.timezone.utc).isoformat()
    elif (len(str(x)) == 13):
        return datetime.datetime.utcfromtimestamp(int(x/1000)).replace(tzinfo = datetime.timezone.utc).isoformat()
    else:
        raise Exception("Unknown timestamp format:", x)

def datetime_to_timestamp(x):
    raise Exception("Please use datetime_to_utctimestamp")

