"""EtlDateTimePathFormat class"""

from omigo_core import utils
from dateutil import parser
import datetime

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

