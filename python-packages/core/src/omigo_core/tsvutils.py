"""utlity methods to read and write tsv data."""

from urllib.parse import urlencode
#from urllib.request import Request, urlopen
#from urllib.error import HTTPError, URLError
from io import BytesIO
import gzip
import json
import time
import zipfile
import requests

# local imports
from omigo_core import tsv
from omigo_core import file_paths_util
from omigo_core import file_paths_data_reader
from omigo_core import file_io_wrapper
from omigo_core import utils
from requests import exceptions
from omigo_hydra import hydra

# TODO: find the difference between ascii and utf-8 encoding
# requests.post doesnt take data properly. Use json parameter.
# TODO: use the local_fs_wrapper to use shared code for reading and writing

# TODO: need to document that a simple union can be achieved by setting def_val_map = {}
def merge(tsv_list, def_val_map = None):
    # validation
    if (len(tsv_list) == 0):
        utils.warn("Error in input. List of tsv is empty")
        return tsv.create_empty()

    # remove tsvs without any columns
    tsv_list = list(filter(lambda x: x.num_cols() > 0, tsv_list))

    # base condition
    if (len(tsv_list) == 0):
        utils.warn("List of tsv is empty. Returning")
        return tsv.create_empty()

    # warn if a huge tsv is found 
    for i in range(len(tsv_list)):
        if (tsv_list[i].size_in_gb() >= 1):
            utils.warn("merge: Found a very big tsv: {} / {}, num_rows: {}, size (GB): {}. max_size_cols_stats: {}".format(
                i + 1, len(tsv_list), tsv_list[i].num_rows(), tsv_list[i].size_in_gb(), str(tsv_list[i].get_max_size_cols_stats())))
            tsv_list[i].show_transpose(1, title = "merge: big tsv")

    # check for valid headers
    # header = tsv_list[0].get_header()
    header_fields = tsv_list[0].get_header_fields()

    # iterate to check mismatch in header
    index = 0
    for t in tsv_list:
        # Use a different method for merging if the header is different
        if (header_fields != t.get_header_fields()):
            header_diffs = get_diffs_in_headers(tsv_list)

            # display warning according to kind of differences found
            if (len(header_diffs) > 0):
                # TODO
                if (def_val_map is None):
                    utils.warn("Mismatch in header at index: {}. Cant merge. Using merge_intersect for common intersection. Some of the differences in header: {}".format(
                        index, str(header_diffs)))
            else:
                utils.warn("Mismatch in order of header fields: {}, {}. Using merge intersect".format(header.split("\t"), t.get_header().split("\t")))

            # return
            return merge_intersect(tsv_list, def_val_map = def_val_map)

        # increment
        index = index + 1

    # simple condition
    if (len(tsv_list) == 1):
        return tsv_list[0]
    else:
        return tsv_list[0].union(tsv_list[1:])

def split_headers_in_common_and_diff(tsv_list):
    common = {}

    # get the counts for each header field
    for t in tsv_list:
        for h in t.get_header_fields():
            if (h not in common.keys()):
                common[h] = 0
            common[h] = common[h] + 1

    # find the columns which are not present everywhere
    non_common = []
    for k, v in common.items():
        if (v != len(tsv_list)):
            non_common.append(k)

    # return
    return sorted(common.keys()), sorted(non_common)

def get_diffs_in_headers(tsv_list):
    common, non_common = split_headers_in_common_and_diff(tsv_list)
    return non_common

def merge_intersect(tsv_list, def_val_map = None):
    # remove zero length tsvs
    tsv_list = list(filter(lambda x: x.num_cols() > 0, tsv_list))

    # base condition
    if (len(tsv_list) == 0):
        raise Exception("List of tsv is empty")

    # boundary condition
    if (len(tsv_list) == 1):
        return tsv_list[0]

    # get the first header
    header_fields = tsv_list[0].get_header_fields()

    # some debugging
    diff_cols = get_diffs_in_headers(tsv_list)
    same_cols = []
    for h in header_fields:
        if (h not in diff_cols):
            same_cols.append(h)

    # print if number of unique headers are more than 1
    if (len(diff_cols) > 0):
        # debug
        utils.debug("merge_intersect: missing columns: {}".format(str(diff_cols)[0:100] + "..."))

        # check which of the columns among the diff have default values
        if (def_val_map is not None):
            # create effective map with empty string as default value
            effective_def_val_map = {}

            # some validation. the default value columns should exist somewhere
            for h in def_val_map.keys():
                # check if all default columns exist
                if (h not in diff_cols and h not in same_cols):
                    raise Exception("Default value for a column given which does not exist:", h)

            # assign empty string to the columns for which default value was not defined
            for h in diff_cols:
                if (h in def_val_map.keys()):
                    utils.trace_once("merge_intersect: assigning default value for {}: {}".format(h, def_val_map[h]))
                    effective_def_val_map[h] = str(def_val_map[h])
                else:
                    utils.trace_once("merge_intersect: assigning empty string as default value to column: {}".format(h))
                    effective_def_val_map[h] = ""

            # get the list of keys in order
            keys_order = []
            for h in header_fields:
                keys_order.append(h)

            # append the missing columns
            for h in diff_cols:
                if (h not in header_fields):
                    keys_order.append(h)

            # create a list of new tsvs
            new_tsvs = []
            for t in tsv_list:
                # TODO: use better design
                t1 = t
                if (def_val_map is not None and len(def_val_map) > 0):
                    for d in diff_cols:
                        t1 = t1.add_const_if_missing(d, effective_def_val_map[d])
                else:
                    t1 = t1.add_empty_cols_if_missing(diff_cols)
                # append to tsv list
                new_tsvs.append(t1.select(keys_order))

            # return after merging. dont call merge recursively as thats a bad design
            return new_tsvs[0].union(new_tsvs[1:]) 
        else:
            # handle boundary condition of no matching cols
            if (len(same_cols) == 0):
                return tsv.create_empty()
            else:
                # create a list of new tsvs
                new_tsvs = []
                for t in tsv_list:
                    new_tsvs.append(t.select(same_cols))

                # return
                return new_tsvs[0].union(new_tsvs[1:])
    else:
        # probably landed here because of mismatch in headers position
        tsv_list2 = []
        for t in tsv_list:
            tsv_list2.append(t.select(same_cols))

        # return
        return merge(tsv_list2)

def read(input_file_or_files, sep = None, def_val_map = None, username = None, password = None, num_par = 0, s3_region = None, aws_profile = None):
    # convert the input to array
    input_files = utils.get_argument_as_array(input_file_or_files)

    # tasks 
    tasks = []

    def __read_inner__(input_file):
        # read file content                               
        lines = file_paths_util.read_file_content_as_lines(input_file, s3_region, aws_profile)
                                                          
        # take header and dat
        header = lines[0]
        data = lines[1:]
            
        # check if a custom separator is defined
        if (sep is not None):
            # check for validation
            for line in lines:
                if ("\t" in line):
                    raise Exception("Cant parse non tab separated file as it contains tab character:", input_file)

            # create header and data
            header = header.replace(sep, "\t")
            data = [x.replace(sep, "\t") for x in data]

        # return
        return tsv.TSV(header, data)

    # create tasks
    for input_file in input_files:
        # check if it is a file or url
        if (input_file.startswith("http")):
            tsv_list.append(read_url_as_tsv(input_file, username = username, password = password))
            tasks.append(utils.ThreadPoolTask(read_url_as_tsv, input_file, username = username, password = password))
        else:
            tasks.append(utils.ThreadPoolTask(__read_inner__, input_file))

    # get result
    tsv_list = utils.run_with_thread_pool(tasks, num_par = num_par, wait_sec = 1)

    # merge and return
    return merge(tsv_list, def_val_map = def_val_map)

def __read_with_filter_transform_select_func__(cols):
    # create a inner function
    def __read_with_filter_transform_select_func_inner__(mp):
        result_mp = {}
        for c in cols:
            if (c in mp.keys()):
                result_mp[c] = str(mp[c])

        # return
        return result_mp

    return __read_with_filter_transform_select_func_inner__

def read_with_filter_transform(input_file_or_files, sep = None, def_val_map = None, filter_transform_func = None, cols = None, transform_func = None, s3_region = None, aws_profile = None):
    # check if cols is defined
    if (filter_transform_func is not None and cols is not None):
        raise Exception("tsvutils: read_with_filter_transform: either of filter_transform_func or cols parameter can be used")

    # use the map function for cols
    if (cols is not None and len(cols) > 0):
        filter_transform_func = __read_with_filter_transform_select_func__(cols)

    # check if filter_transform_func is defined
    if (filter_transform_func is None):
        xtsv = read(input_file_or_files, sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)

        # apply transform_func if defined
        xtsv_transform = transform_func(xtsv) if (transform_func is not None) else xtsv

        # return
        return xtsv_transform
    else:
        # resolve input
        input_files = utils.get_argument_as_array(input_file_or_files)

        # initialize result
        tsv_list = []

        # common keys
        common_keys = {}

        # iterate over all input files
        for input_file in input_files:
            # read the file
            x = read(input_file, sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)

            # update the common
            for h in x.get_header_fields():
                if (h not in common_keys.keys()):
                    common_keys[h] = 0
                common_keys[h] = common_keys[h] + 1

            # gather maps of maps
            result_maps = []
            keys = {}

            # iterate over the records of each map and generate a new one
            for mp in x.to_maps():
                mp2 = filter_transform_func(mp)
                if (mp2 is not None):
                    if (len(mp2) > 0):
                        result_maps.append(mp2)
                    for k in mp2.keys():
                        keys[k] = 1

            # check for empty maps
            if (len(keys) > 0):
                # output keys
                keys_sorted = []
                first_file = read(input_files[0], sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)
                for h in first_file.get_header_fields():
                    if (h in keys.keys()):
                        keys_sorted.append(h)

                # new header and data
                header2 = "\t".join(keys_sorted)
                data2 = []

                # iterate and generate header and data
                for mp in result_maps:
                    fields = []
                    for k in keys_sorted:
                        fields.append(mp[k])
                    data2.append("\t".join(fields))

                # debugging
                utils.trace("tsvutils: read_with_filter_transform: file read: {}, after filter num_rows: {}".format(input_file, len(data2)))

                # result tsv
                xtsv = tsv.TSV(header2, data2)

                # apply transformation function if defined
                xtsv_transform = transform_func(xtsv) if (transform_func is not None) else xtsv
                tsv_list.append(xtsv_transform)

        # Do a final check to see if all tsvs are empty
        if (len(tsv_list) > 0):
            # call merge on tsv_list
            return merge(tsv_list)
        else:
            # create an empty data tsv file with common header fields
            header_fields = []
            first_file = read(input_files[0], sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)
            for h in first_file.get_header_fields():
                if (common_keys[h] == len(input_files)):
                    header_fields.append(h)

            new_header = "\t".join(header_fields)
            new_data = []

            # return
            return tsv.TSV(new_header, new_data)

# TODO: replace this by etl_ext
def read_by_date_range(path, start_date_str, end_date_str, prefix, s3_region = None, aws_profile = None, granularity = "daily"):
    utils.warn_once("read_by_date_range: probably Deprecated")
    # read filepaths
    filepaths = file_paths_util.read_filepaths(path, start_date_str, end_date_str, prefix, s3_region, aws_profile, granularity)

    # check for headers validity
    if (file_paths_util.has_same_headers(filepaths, s3_region, aws_profile) == False):
        utils.warn("Mismatch in headers for different days. Choose the right date range: start: {}, end: {}".format(start_date_str, end_date_str))
        return None

    # read individual tsvs
    tsv_list = []
    for filepath in filepaths:
        x = read(filepath)
        tsv_list.append(x)

    # combine all together
    if (len(tsv_list) == 0):
        return None
    elif (len(tsv_list) == 1):
        return tsv_list[0]
    else:
        return tsv_list[0].union(tsv_list[1:])

def load_from_dir(path, start_date_str, end_date_str, prefix, s3_region = None, aws_profile = None, granularity = "daily"):
    # read filepaths
    filepaths = file_paths_util.read_filepaths(path, start_date_str, end_date_str, prefix, s3_region, aws_profile, granularity)
    return load_from_files(filepaths, s3_region, aws_profile)

def load_from_files(filepaths, s3_region, aws_profile):
    # check for headers validity
    if (file_paths_util.has_same_headers(filepaths, s3_region, aws_profile) == False):
        print("Invalid headers.")
        return None

    # initialize the file reader
    file_reader = file_paths_data_reader.FilePathsDataReader(filepaths, s3_region, aws_profile)

    # get header
    header = file_reader.get_header()
    data = []

    # get data
    while file_reader.has_next():
        # read next record
        line = file_reader.next()
        data.append(line)

    # close
    file_reader.close()

    return tsv.TSV(header, data)

# TODO: use explode_json
def load_from_array_of_map(map_arr):
    # take a union of all keys
    keys = {}

    # copy map array and remove any white spaces
    map_arr2 = []
    for mp in map_arr:
        mp2 = {}
        for k in mp.keys():
            # for robustness remove any special white space characters
            k2 = utils.replace_spl_white_spaces_with_space(k)
            v = mp[k]

            # check for the type of value
            if (isinstance(v, (str))):
                v2 = utils.replace_spl_white_spaces_with_space(v)
            elif (isinstance(v, (list))):
                v2 = ",".join(list([utils.replace_spl_white_spaces_with_space(t1) for t1 in v]))
            elif (isinstance(v, (dict))):
                v2 = utils.url_encode(json.dumps(v))
                k2 = "{}:json_encoded".format(k2)
            elif (isinstance(v, (int))):
                v2 = str(v)
            else:
                v2 = str(v)

            # assign to map 
            mp2[k2] = v2

        # append
        map_arr2.append(mp2)

    # iterate over all maps
    for mp in map_arr2:
        for k in mp.keys():
            keys[k] = 1

    # sort the keys alphabetically
    sorted_keys = sorted(list(set(keys.keys())))

    # create header
    header = "\t".join(sorted_keys)
    header_fields = header.split("\t")
    header_map = {}
    for i in range(len(header_fields)):
        h = header_fields[i]
        header_map[h] = i

    data = []
    # create data
    for mp in map_arr2:
        fields = []
        for k in header_fields:
            v = ""
            if (k in mp.keys()):
                # read as string and replace any newline or tab characters
                v = utils.replace_spl_white_spaces_with_space(mp[k])

            # append
            fields.append(v)

        # create line
        line = "\t".join(fields)
        data.append(line)

    # create tsv
    return tsv.TSV(header, data).validate()

def save_to_file(*args, **kwargs):
    utils.noop_after_n_warnings("Deprecated. Use omigo_hydra instead", hydra.save_to_file, *args, **kwargs)

def check_exists(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use omigo_hydra instead")
    return hydra.check_exists(*args, **kwargs)

# Deprecated methods. Use wsclient package instead
def read_url_json(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use wsclient instead", num_warnings = 100, sleep_secs = 300)
    return wsclient.read_url_json(*args, **kwargs)

def read_url_response(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use wsclient instead", num_warnings = 100, sleep_secs = 300)
    return wsclient.read_url_response(*args, **kwargs)

def read_url(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use wsclient instead", num_warnings = 100, sleep_secs = 300)
    return wsclient.read_url(*args, **kwargs)

def read_url_as_tsv(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use wsclient instead", num_warnings = 100, sleep_secs = 300)
    return wsclient.read_url_as_tsv(*args, **kwargs)


