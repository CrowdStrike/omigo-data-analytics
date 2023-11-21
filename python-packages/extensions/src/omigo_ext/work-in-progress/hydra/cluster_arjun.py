# Integration parameters for anything with omigo_arjun

# Meta parameters
OMIGO_ARJUN_START_TS = "omigo.arjun.start_ts"
OMIGO_ARJUN_END_TS = "omigo.arjun.end_ts"
OMIGO_ARJUN_EVENT_TS = "omigo.arjun.event_ts"
OMIGO_ARJUN_BASE_PATH = "omigo.arjun.base_path"

OMIGO_ARJUN_INPUT_FILE = "omigo.arjun.input_file"
OMIGO_ARJUN_OUTPUT_FILE = "omigo.arjun.output_file"

# ETL specific header parameters
OMIGO_ARJUN_ETL_PATH_PREFIX = ".omigo.arjun.etl.path."
OMIGO_ARJUN_ETL_START_TS = ".omigo.arjun.etl.start_ts"
OMIGO_ARJUN_ETL_END_TS = ".omigo.arjun.etl.end_ts"
# OMIGO_ARJUN_ETL_USE_FULL_DATA = ".omigo.arjun.etl.use_full_data" # TODO: move this to context
OMIGO_ARJUN_ETL_FILE_PREFIX = "output"

# meta parameters as template
OMIGO_ARJUN_START_TS_TEMPLATE = "{" + OMIGO_ARJUN_START_TS + "}"
OMIGO_ARJUN_END_TS_TEMPLATE = "{" + OMIGO_ARJUN_END_TS + "}"
OMIGO_ARJUN_BASE_PATH_TEMPLATE = "{" + OMIGO_ARJUN_BASE_PATH + "}"

# TODO: these templates dont support multiple inputs
OMIGO_ARJUN_INPUT_FILE_TEMPLATE = "{" + OMIGO_ARJUN_INPUT_FILE + "}" 
OMIGO_ARJUN_OUTPUT_FILE_TEMPLATE = "{" + OMIGO_ARJUN_OUTPUT_FILE + "}"

def get_etl_path_by_id(strid):
    return "{}{}".format(OMIGO_ARJUN_ETL_PATH_PREFIX, strid)

def is_etl_path(path):
    # check for none
    if (path is None):
        return False
  
    # return
    return path.startswith(OMIGO_ARJUN_ETL_PATH_PREFIX)

def parse_id_from_etl_path(path):
    # check for none
    if (path is None):
        return None 
  
    # get index
    index = path.find(OMIGO_ARJUN_ETL_PATH_PREFIX)
    if (index != -1):
        return path[index+1:]
    else:
        return None


