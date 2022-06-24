"""EtlDateTimePathFormat class"""

from omigo_core import utils, etl

# Deprecated. Use etl module instead
class EtlDateTimePathFormat:
    def __init__(self, *args, **kwargs):
        utils.warn("use etl.EtlDateTimePathFormat module instead")
        self.__inner__ = etl.EtlDateTimePathFormat(*args, **kwargs)

    def get_correct_end_datetime(self):
        utils.warn("use etl.EtlDateTimePathFormat module instead")
        return self.__inner__.get_correct_end_datetime()

    def to_string(self):
        utils.warn("use etl.EtlDateTimePathFormat module instead")
        return self.__inner__.to_string()

def get_matching_etl_date_time_path(*args, **kwargs):
    utils.warn("use etl module instead")
    return etl.get_matching_etl_date_time_path(*args, **kwargs)

def get_etl_date_str_from_ts(*args, **kwargs):
    utils.warn("use etl module instead")
    return etl.get_etl_date_str_from_ts(*args, **kwargs)

def get_etl_file_base_name_by_ts(*args, **kwargs):
    utils.warn("use etl module instead")
    return etl.get_etl_file_base_name_by_ts(*args, **kwargs)

def scan_by_datetime_range(*args, **kwargs):
    utils.warn("use etl module instead")
    return etl.scan_by_datetime_range(*args, **kwargs)

def get_file_paths_by_datetime_range(*args, **kwargs):
    utils.warn("use etl module instead")
    return etl.get_file_paths_by_datetime_range(*args, **kwargs)

