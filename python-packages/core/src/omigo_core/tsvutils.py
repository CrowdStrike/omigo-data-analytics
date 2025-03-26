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

def read(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use hydra instead", num_warnings = 100, sleep_secs = 300)
    return hydra.read(*args, **kwargs)

def read_with_filter_transform(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use hydra instead", num_warnings = 100, sleep_secs = 300)
    return hydra.read_with_filter_transform(*args, **kwargs)

def read_by_date_range(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use hydra instead", num_warnings = 100, sleep_secs = 300)
    return hydra.read_by_date_range(*args, **kwargs)

def load_from_dir(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use hydra instead", num_warnings = 100, sleep_secs = 300)
    return hydra.load_from_dir(*args, **kwargs)

def load_from_files(*args, **kwargs):
    utils.rate_limit_after_n_warnings("Deprecated. Use hydra instead", num_warnings = 100, sleep_secs = 300)
    return hydra.load_from_files(*args, **kwargs)

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


