from omigo_core import utils, timefuncs
import datetime

# helper function to add relative time option in splunk and logscale
def resolve_time_str(x):
    # check for specific syntax with now
    if (x.startswith("now")):
        x = x.replace(" ", "")
        base_time = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)
        diff_sec = None
        # check if there are any diff units
        if (x == "now"):
            diff_sec = 0
        else:
            # validation for parsing logic
            if (x.startswith("now-") == False):
                raise Exception("Unknown operator against now: ", x)

            # take the diff part
            diffstr = x[len("now-"):]

            # unit is single letter, 'd', 'h', 'm', 's'
            unit = diffstr[-1]

            # how many of units to apply
            count = int(diffstr[0:-1])
            if (unit == "d"):
                diff_sec = count * 86400
            elif (unit == "h"):
                diff_sec = count * 3600
            elif (unit == "m"):
                diff_sec = count * 60
            elif (unit == "s"):
                diff_sec = count * 1
            else:
                raise Exception("Unknown time unit:", parts[1])

        # return base_time minus diff
        return timefuncs.utctimestamp_to_datetime_str(int(base_time.timestamp()) - diff_sec)
    else:
        return timefuncs.utctimestamp_to_datetime_str(timefuncs.datetime_to_utctimestamp_sec(x))

