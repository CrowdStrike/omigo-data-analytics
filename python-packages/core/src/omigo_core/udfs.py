"""library of function to be used in calling lambda functions"""

import statistics
import numpy as np
from dateutil import parser
import datetime
from omigo_core import utils

def file_base_name(x):
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

    # adding check for atleast 2 elements before calling the statistics package
    if (len(vs) < 2):
        return 0

    # return
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

# TODO: The semantics are not clear
def uniq_count(vs):
    vs2 = list(filter(lambda t: t.strip() != "", vs))
    return len(set(vs2))

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
    utils.warn_once("min_str is deprecated. Use minstr")
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

