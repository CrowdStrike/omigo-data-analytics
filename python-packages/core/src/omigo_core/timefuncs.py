"""library of function to be used for working with time"""

from dateutil import parser
import datetime
from omigo_core import utils

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
    return int(datetime_to_utctimestamp_sec(x) * 1000)

# TODO. better naming
def datetime_to_utctimestamp(x):
    # use datetime_to_utctimestamp_sec
    utils.warn_once("datetime_to_utctimestamp: Deprecated. Use datetime_to_utctimestamp_sec instead")
    return datetime_to_utctimestamp_sec(x)

def datetime_to_utctimestamp_sec(x):
    # convert this to string first for failsafe
    x = str(x)

    # 2022-05-20T05:00:00+00:00
    if (x.endswith("UTC") or x.endswith("GMT") or x.endswith("Z") or x.endswith("+00:00")):
        return int(parser.parse(x).timestamp())
    elif (len(x) == 10 and x.find("-") != -1):
        # 2021-11-01
        x = x + "T00:00:00Z"
        return int(parser.parse(x).timestamp())
    elif (len(x) == 19 and (x[10] == "T" or x[10] == " ")):
        # 2021-11-01T00:00:00
        x = x + "Z"
        if (x[10] == " "):
            x = x.replace(" ", "T")
        return int(parser.parse(x).timestamp())
    elif (len(x) == 23 and x[19] == "."):
        # 2021-11-01T00:00:00.000
        x = x + "Z"
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
    elif (len(x) == 28 and x[-5] == "+" and x[-9] == "."):
        # 2025-05-08T20:03:35.000+0000
        return int(float(parser.parse(x).timestamp()))
    else:
        raise Exception("Unknown date format. Problem with UTC: '{}'".format(x))

# TODO: Converts seconds format only. Even the original time in milliseconds will return seconds format
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

# Its utc so removed the last timezone. TODO: Keep the UTC or Z
def utctimestamp_millis_to_datetime_str(x):
    result = utctimestamp_to_datetime(x).isoformat()
    if (result.endswith("UTC")):
        return result[0:23]
    else:
        return result

def datetime_to_timestamp(x):
    raise Exception("Please use datetime_to_utctimestamp")

def get_utctimestamp_sec():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())

def get_utctimestamp_millis():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

def datestr_to_datetime(x):
    return utctimestamp_to_datetime(datetime_to_utctimestamp_sec(x))

