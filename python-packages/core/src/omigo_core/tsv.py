"""TSV Class"""
import re
import math
import pandas as pd
import random
import json
from omigo_core import tsvutils
from omigo_core import utils
import sys

class TSV:
    """This is the main data processing class to apply different filter and transformation functions
    on tsv data and get the results. The design is more aligned with functional programming where
    each step generates a new copy of the data"""

    header = None
    header_map = None
    header_index_map = None
    header_fields = None
    data = None

    # constructor
    def __init__(self, header, data):
        # initialize header and data
        self.header = header
        self.data = data

        # create map of name->index and index->name
        self.header_fields = self.header.split("\t")
        self.header_map = {}
        self.header_index_map = {}

        # validation
        for h in self.header_fields:
            if (len(h) == 0):
                utils.warn("Zero length header fields:" + str(self.header_fields))

        # create hashmap
        for i in range(len(self.header_fields)):
            h = self.header_fields[i]

            # validation
            if (h in self.header_map.keys()):
                raise Exception("Duplicate header key:" + str(self.header_fields))

            self.header_map[h] = i
            self.header_index_map[i] = h

        # basic validation
        if (len(data) > 0 and len(data[0].split("\t")) != len(self.header_fields)):
            raise Exception("Header length is not matching with data length:", len(self.header_fields), len(data[0].split("\t")), str(self.header_fields), str(data[0].split("\t")))

    # debugging
    def to_string(self):
        return "Header: {}, Data: {}".format(str(self.header_map), str(len(self.data)))

    # check data format
    def validate(self):
        # data validation
        count = 0
        for line in self.data:
            count = count + 1
            fields = line.split("\t")
            if (len(fields) != len(self.header_fields)):
                raise Exception("Header length is not matching with data length. position: {}, len(header): {}, header: {}, len(fields): {}, fields: {}".format(
                    count, len(self.header_fields), self.header_fields, len(fields), str(fields)))

        # return
        return self

    def has_col(self, col):
        # validate xcol
        return col in self.header_map.keys()

    # cols is array of string
    def select(self, col_or_cols, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("select: empty tsv")

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(col_or_cols)
        indexes = self.__get_col_indexes__(matching_cols)

        # create new header
        new_header = "\t".join(matching_cols)

        # create new data
        counter = 0
        new_data = []
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("select: [1/1] selecting columns", inherit_message, counter, len(self.data))

            fields = line.split("\t")
            new_fields = []
            for i in indexes:
                if (i >= len(fields)):
                    raise Exception("Invalid index: ", col_or_cols, matching_cols, indexes, line, fields, len(fields), len(self.header_fields), self.header_map)
                new_fields.append(fields[i])

            new_data.append("\t".join(new_fields))

        # return
        return TSV(new_header, new_data)

    def values_not_in(self, col, values, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": values_not_in" if (inherit_message != "") else "values_not_in"
        return self.filter([col], lambda x: x not in values, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def values_in(self, col, values, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": values_in" if (inherit_message != "") else "values_in"
        return self.filter([col], lambda x: x in values, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def not_match(self, col, pattern, ignore_if_missing = False, inherit_message = ""):
        utils.warn("Please use not_regex_match instead")
        return self.not_regex_match(col, pattern, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message)

    def not_regex_match(self, col, pattern, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": not_regex_match" if (inherit_message != "") else "not_regex_match"
        return self.regex_match(col, pattern, condition = False, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def match(self, col, pattern, ignore_if_missing = False, inherit_message = ""):
        utils.warn("Please use regex_match instead")
        return self.regex_match(col, pattern, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message)

    def regex_match(self, col, pattern, condition = True, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": regex_match" if (inherit_message != "") else "regex_match"
        return self.filter([col], lambda x: (re.match(pattern, x) is not None) == condition, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def not_eq(self, col, value, ignore_if_missing = False, inherit_message = ""):
        utils.warn("This api can have side effects because of implicit data types conversion in python. Use not_eq_int, not_eq_str or not_eq_float")
        inherit_message2 = inherit_message + ": not_eq" if (inherit_message != "") else "not_eq"
        return self.filter([col], lambda x: x != value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def eq(self, col, value, ignore_if_missing = False, inherit_message = ""):
        utils.warn("This api can have side effects because of implicit data types conversion in python. Use eq_int, eq_str or eq_float")
        return self.filter([col], lambda x: x == value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def eq_int(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": eq_int" if (inherit_message != "") else "eq_int"
        return self.filter([col], lambda x: int(float(x)) == value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def eq_float(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": eq_float" if (inherit_message != "") else "eq_float"
        return self.filter([col], lambda x: float(x) == value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def eq_str(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": eq_str" if (inherit_message != "") else "eq_str"
        return self.filter([col], lambda x: str(x) == str(value), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def not_eq_str(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": not_eq_str" if (inherit_message != "") else "not_eq_str"
        return self.filter([col], lambda x: str(x) != str(value), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def is_nonzero(self, col, ignore_if_missing = False, inherit_message = ""):
        utils.warn("Deprecated. Use is_nonzero_float() instead")
        inherit_message2 = inherit_message + ": is_nonzero" if (len(inherit_message) > 0) else "is_nonzero"
        return self.is_nonzero_float(col, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def is_nonzero_int(self, col, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": is_nonzero_int" if (len(inherit_message) > 0) else "is_nonzero_int"
        return self.filter([col], lambda x: int(x) != 0, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def is_nonzero_float(self, col, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": is_nonzero_float" if (len(inherit_message) > 0) else "is_nonzero_float"
        return self.filter([col], lambda x: float(x) != 0, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def lt_str(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": lt_str" if (len(inherit_message) > 0) else "lt_str"
        return self.filter([col], lambda x: x < value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def le_str(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": le_str" if (len(inherit_message) > 0) else "le_str"
        return self.filter([col], lambda x: x <= value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def gt_str(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": gt_str" if (len(inherit_message) > 0) else "gt_str"
        return self.filter([col], lambda x: x > value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def ge_str(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": ge_str" if (len(inherit_message) > 0) else "ge_str"
        return self.filter([col], lambda x: x >= value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def gt(self, col, value, ignore_if_missing = False, inherit_message = ""):
        utils.warn("Deprecated. Use gt_float() instead")
        inherit_message2 = inherit_message + ": gt" if (len(inherit_message) > 0) else "gt"
        return self.gt_float(col, value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def gt_int(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": gt_int" if (len(inherit_message) > 0) else "gt_int"
        return self.filter([col], lambda x: int(float(x)) > int(float(value)), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def gt_float(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": gt_float" if (len(inherit_message) > 0) else "gt_float"
        return self.filter([col], lambda x: float(x) > float(value), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def ge(self, col, value, ignore_if_missing = False, inherit_message = ""):
        utils.warn("Deprecated. Use ge_float() instead")
        inherit_message2 = inherit_message + ": ge" if (len(inherit_message) > 0) else "ge"
        return self.ge_float(col, value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def ge_int(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": ge_int" if (len(inherit_message) > 0) else "ge_int"
        return self.filter([col], lambda x: int(float(x)) >= int(float(value)), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def ge_float(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": ge_float" if (len(inherit_message) > 0) else "ge_float"
        return self.filter([col], lambda x: float(x) >= float(value), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def lt(self, col, value, ignore_if_missing = False, inherit_message = ""):
        utils.warn("Deprecated. Use lt_float() instead")
        inherit_message2 = inherit_message + ": lt" if (len(inherit_message) > 0) else "lt"
        return self.lt_float(col, value, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def lt_int(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": lt_int" if (len(inherit_message) > 0) else "lt_int"
        return self.filter([col], lambda x: int(float(x)) < int(float(value)), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def lt_float(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": lt_float" if (len(inherit_message) > 0) else "lt_float"
        return self.filter([col], lambda x: float(x) < float(value), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def le(self, col, value, ignore_if_missing = False, inherit_message = ""):
        utils.warn("Deprecated. Use le_float() instead")
        inherit_message2 = inherit_message + ": le" if (len(inherit_message) > 0) else "le"
        return self.filter([col], lambda x: float(x) <= float(value), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def le_int(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": le_int" if (len(inherit_message) > 0) else "le_int"
        return self.filter([col], lambda x: int(float(x)) <= int(float(value)), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def le_float(self, col, value, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": le_float" if (len(inherit_message) > 0) else "le_float"
        return self.filter([col], lambda x: float(x) <= float(value), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def startswith(self, col, prefix, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": startswith" if (len(inherit_message) > 0) else "startswith"
        return self.filter([col], lambda x: x.startswith(prefix), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def not_startswith(self, col, prefix, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": not_startswith" if (len(inherit_message) > 0) else "not_startswith"
        return self.exclude_filter([col], lambda x: x.startswith(prefix), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def endswith(self, col, suffix, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": endswith" if (len(inherit_message) > 0) else "endswith"
        return self.filter([col], lambda x: x.endswith(suffix), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def not_endswith(self, col, suffix, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": not_endswith" if (len(inherit_message) > 0) else "not_endswith"
        return self.exclude_filter([col], lambda x: x.endswith(suffix), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def replace_str_inline(self, cols, old_str, new_str, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": replace_str_inline" if (len(inherit_message) > 0) else "replace_str_inline"
        return self.transform_inline(cols, lambda x: x.replace(old_str, new_str), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def group_count(self, cols, prefix = "group", collapse = True, precision = 6, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("group_count: empty tsv")

        # find the matching cols and indexes
        cols = self.__get_matching_cols__(cols)

        # define new columns
        new_count_col = prefix + ":count"
        new_ratio_col = prefix + ":ratio"

        # validation and suggestion
        if (new_count_col in cols or new_ratio_col in cols):
            raise Exception("Use a different prefix than: {}".format(prefix))

        # call aggregate with collapse=False
        inherit_message2 = inherit_message + ":group_count" if (inherit_message != "") else "group_count"
        return self.aggregate(cols, [cols[0]], [len], collapse = collapse, inherit_message = inherit_message2) \
            .rename(cols[0] + ":len", new_count_col) \
            .transform([new_count_col], lambda x: str(int(x) / len(self.data)), new_ratio_col, inherit_message = inherit_message2) \
            .reverse_sort(new_count_col) \
            .apply_precision(new_ratio_col, precision, inherit_message = inherit_message2)

    def ratio(self, col1, col2, new_col, default = 0.0, precision = 6, inherit_message = ""):
        return self \
            .transform([col1, col2], lambda x, y: float(x) / float(y) if (float(y) != 0) else default, new_col) \
            .apply_precision(new_col, precision, inherit_message = inherit_message)

    def ratio_const(self, col, denominator, new_col, precision = 6, inherit_message = ""):
        return self \
            .transform([col], lambda x: float(x) / float(denominator) if (float(denominator) != 0) else default, new_col) \
            .apply_precision(new_col, precision, inherit_message = inherit_message)

    def apply_precision(self, cols, precision, inherit_message = ""):
        inherit_message2 = inherit_message + ": apply_precision" if (len(inherit_message) > 0) else "apply_precision"
        return self.transform_inline(cols, lambda x: ("{:." + str(precision) + "f}").format(float(x)), inherit_message = inherit_message2)

    # TODO: use skip_rows for better name
    def skip(self, count):
        return TSV(self.header, self.data[count:])

    def skip_rows(self, count):
        return self.skip(count)

    def last(self, count):
        # check boundary conditions
        if (count > len(self.data)):
            count = len(self.data)

        # return
        return TSV(self.header, self.data[-count:])

    def take(self, count):
        # return result
        if (count > len(self.data)):
            count = len(self.data)

        return TSV(self.header, self.data[0:count])

    def distinct(self):
        # create variables
        new_data = []
        key_map = {}

        # iterate
        for line in self.data:
            if (line not in key_map.keys()):
                key_map[line] = 1
                new_data.append(line)

        # return
        return TSV(self.header, new_data)

    # TODO: use drop_cols instead coz of better name
    def drop(self, col_or_cols, ignore_if_missing = False, inherit_message = ""):
        utils.warn("use drop_cols instead coz of better name")

        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("drop: empty tsv", ignore_if_missing)
            return self

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(col_or_cols, ignore_if_missing = ignore_if_missing)

        # find the columns that dont match
        non_matching_cols = []
        for h in self.header_fields:
            if (h not in matching_cols):
                non_matching_cols.append(h)

        # return
        inherit_message2 = inherit_message + ": drop" if (len(inherit_message) > 0) else "drop"
        return self.select(non_matching_cols, inherit_message = inherit_message2)

    def drop_cols(self, col_or_cols, inherit_message = ""):
        return self.drop(col_or_cols, inherit_message)

    def drop_if_exists(self, col_or_cols, inherit_message = ""):
        return self.drop(col_or_cols, ignore_if_missing = True, inherit_message = inherit_message)

    # TODO: the select_cols is not implemented properly
    def window_aggregate(self, win_col, agg_cols, agg_funcs, winsize, select_cols = None, sliding = False, collapse = True, suffix = "", precision = 2, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("window_aggregate: empty tsv")

        # get the matching cols
        if (select_cols is None):
            select_cols = []
        select_cols = self.__get_matching_cols__(select_cols)

        # do validation on window column. All values should be unique
        if (len(self.col_as_array(win_col)) != len(self.col_as_array_uniq(win_col))):
            utils.warn("The windowing column has non unique values: total: {}, uniq: {}. The results may not be correct.".format(len(self.col_as_array(win_col)),  len(self.col_as_array_uniq(win_col))))

        # this takes unique values for agg column, split them into windows and then run the loop
        win_col_values = sorted(list(set(self.col_as_array(win_col))))

        # store the number of values
        num_win_col_values = len(win_col_values)

        # find number of windows
        num_windows = int(math.ceil(1.0 * num_win_col_values / winsize)) if (sliding == False) else (num_win_col_values - (winsize - 1))

        # map to the window index
        win_mapping = {}
        win_names_mapping = {}

        # assign index
        for i in range(num_win_col_values):
            win_col_val = win_col_values[i]
            win_indexes = []
            if (sliding == False):
                win_index = int(i / winsize)
                win_indexes.append(win_index)
            else:
                for i1 in range(winsize):
                    win_index = i - (winsize - 1) + i1
                    if (win_index >= 0 and (num_win_col_values - win_index) >= winsize):
                        win_indexes.append(win_index)

            # assign the mapping indexes
            win_mapping[win_col_val] = win_indexes

        # assign window column range
        for win_index in range(num_windows):
            if (sliding == True):
                win_names_mapping[win_index] = (win_col_values[win_index], win_col_values[win_index + winsize - 1])
            else:
                start_index = win_index * winsize
                end_index = int(min(num_win_col_values - 1, win_index * winsize + winsize - 1))
                win_names_mapping[win_index] = (win_col_values[start_index], win_col_values[end_index])

        # transform and normalize the value of win_col
        suffix2 = suffix if (suffix != "") else "window_aggregate"
        new_win_col = win_col + ":" + suffix2
        new_header = self.header + "\t" + new_win_col

        new_data = []

        # iterate over data
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("window_aggregate: [1/1] calling function", inherit_message, counter, len(self.data))

            # parse data
            fields = line.split("\t")
            win_value = fields[self.header_map[win_col]]
            win_indexes = win_mapping[win_value]

            # explode for multiple windows
            for win_index in win_indexes:
                (namex, namey) = win_names_mapping[win_index]
                namexy = namex + " - " + namey
                new_data.append(line + "\t" + str(namexy))

        # return
        if (collapse == True):
            cols2 = select_cols
            cols2.append(win_col)
            return TSV(new_header, new_data) \
                .drop(win_col) \
                .rename(new_win_col, win_col) \
                .aggregate(cols2, agg_cols, agg_funcs, collapse)
        else:
            cols2 = select_cols
            cols2.append(new_win_col)
            return TSV(new_header, new_data) \
                .aggregate(cols2, agg_cols, agg_funcs, collapse, precision)

    # The signature for agg_func is func(list_of_maps). Each map will get the agg_cols
    def group_by_key(self, grouping_cols, agg_cols, agg_func, suffix = "", collapse = True, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("group_by_key: empty tsv")

        # resolve grouping and agg_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)
        agg_cols = self.__get_matching_cols__(agg_cols)

        # check for validity
        if (len(grouping_cols) == 0):
            raise Exception("No input columns:", grouping_cols)

        # validate grouping cols
        for c in grouping_cols:
            if (c not in self.header_map.keys()):
                raise Exception("grouping col not found:", c, ", columns:", self.header_fields)

        # validate agg cols
        for c in agg_cols:
            if (c not in self.header_map.keys()):
                raise Exception("agg col not found:", c, ", columns:", self.header_fields)

        # group all the values in the key
        grouped = {}
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("group_by_key: [1/3] grouping: progress", inherit_message, counter, len(self.data))

            # parse data
            fields = line.split("\t")

            # create grouping key
            keys = []
            values_map = {}
            for g in grouping_cols:
                keys.append(fields[self.header_map[g]])

            for a in agg_cols:
                values_map[a] = fields[self.header_map[a]]

            keys_str = "\t".join(keys)
            if (keys_str not in grouped.keys()):
                grouped[keys_str] = []

            grouped[keys_str].append(values_map)

        # apply the agg func
        grouped_agg = {}
        counter = 0
        for k, vs in grouped.items():
            # report progress
            counter = counter + 1
            utils.report_progress("group_by_key: [2/3] grouping func: progress", inherit_message, counter, len(grouped))

            # get fields
            vs_map = agg_func(vs)
            grouped_agg[k] = vs_map

        # determine the set of keys in the aggregation function output
        agg_out_keys = {}

        # check for special condition for empty data
        if (len(grouped) > 0):
            for k, vs in grouped_agg.items():
                for k2 in vs.keys():
                    agg_out_keys[k2] = 1
        else:
            dummy_response = agg_func([])
            for k in dummy_response.keys():
                agg_out_keys[k] = 1

        # validate that none of the agg_func output keys are conflicting with the original tsvs. TODO
        utils.print_code_todo_warning("Removing this condition for checking of duplicate names. They are already given a suffix so there is no clash.")
        for k in agg_out_keys.keys():
            if (k in self.header_map.keys()):
                utils.print_code_todo_warning("TODO: Old check: Agg func can not output keys that have the same name as original columns: {}, {}".format(k, str(self.header_fields)))

        # create an ordered list of agg output keys
        new_cols = sorted(list(agg_out_keys.keys()))
        new_cols_names = []
        name_suffix = suffix if (suffix != "") else get_func_name(agg_func)
        for nc in new_cols:
            new_cols_names.append(nc + ":" + name_suffix)

        # check for collapse flag and add the agg func value
        result = {}

        # create header
        new_header = None
        if (collapse == True):
            new_header = "\t".join(utils.merge_arrays([grouping_cols, new_cols_names]))
        else:
            new_header = "\t".join(utils.merge_arrays([self.header_fields, new_cols_names]))

        # create data
        new_data = []
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("group_by_key: [3/3] generating data", inherit_message, counter, len(self.data))

            # process data
            fields = line.split("\t")

            # create grouping key
            keys = []
            for g in grouping_cols:
                keys.append(fields[self.header_map[g]])

            keys_str = "\t".join(keys)

            # check output data
            if (len(grouped_agg[keys_str]) != len(new_cols)):
                raise Exception("Error in data and number of output cols:", grouped_agg[keys_str], new_cols)

            # get the new values in the correct order
            new_col_values = []
            vs = grouped_agg[keys_str]
            for c in new_cols:
                new_col_values.append(str(vs[c]))

            # generate the string for new values
            v = "\t".join(new_col_values)

            # add result according to collapse flag
            if (collapse == True):
                if (keys_str not in result.keys()):
                    # append to the output data
                    new_line = keys_str + "\t" + str(v)
                    new_data.append(new_line)
                    result[keys_str] = 1
            else:
                new_line = line + "\t" + str(v)
                new_data.append(new_line)

        return TSV(new_header, new_data)

    # FIXME
    def arg_min(self, grouping_cols, argcols, valcols, suffix = "arg_min", topk = 1, sep = "|", collapse = True):
        utils.warn("arg_min is not implemented correctly. Too complicated")
        return self.__arg_min_or_max_common__(grouping_cols, argcols, valcols, suffix, topk, sep, -1, collapse = collapse)

    def arg_max(self, grouping_cols, argcols, valcols, suffix = "arg_max", topk = 1, sep = "|", collapse = True):
        utils.warn("arg_max is not implemented correctly. Too complicated")
        return self.__arg_min_or_max_common__(grouping_cols, argcols, valcols, suffix, topk, sep, 1, collapse = collapse)

    # grouping_cols are for grouping
    # argcols which are returned where valcols values are max or min
    # suffix is added to both arg and val. arg are suffixed as :arg, values are suffixed as val1, val2 upto topk
    def __arg_min_or_max_common__(self, grouping_cols, argcols, valcols, suffix, topk, sep, sign, collapse = False):
        grouping_cols = self.__get_matching_cols__(grouping_cols)
        argcols = self.__get_matching_cols__(argcols)
        valcols = self.__get_matching_cols__(valcols)

        def __arg_max_grouping_func__(vs):
            # initialize
            max_keys = []
            for i in range(len(argcols)):
                max_keys.append([])
            max_values = []
            for i in range(len(valcols)):
                max_values.append(sign * float('-inf'))

            # iterate over all values
            for mp in vs:
                # read keys
                keys = []
                for i in range(len(argcols)):
                    keys.append(mp[argcols[i]])

                # read values
                values = []
                for i in range(len(valcols)):
                    values.append(sign * float(mp[valcols[i]]))

                # check if a new max has been found
                found = False
                for i in range(len(values)):
                    if (max_values[i] < values[i]):
                        # found a new max
                        for j in range(len(keys)):
                            max_keys[j] = [str(keys[j])]
                        for j in range(len(values)):
                            max_values[j] = values[j]
                        found = True
                        break
                    elif (max_values[i] > values[i]):
                        found = True
                        break

                # check for value of found. If it is still true, then it means multiple matches
                if (found == False):
                    for i in range(len(keys)):
                        max_keys[i].append(str(keys[i]))

            # check for topk
            result = {}
            for i in range(len(argcols)):
                result[argcols[i] + ":arg"] = sep.join(max_keys[i][0:topk])
            for i in range(len(valcols)):
                result[valcols[i] + ":val" + str(i+1)] = str(max_values[i])

            # return
            return result

        # combine both columns
        combined_cols = []
        for k in argcols:
            combined_cols.append(k)
        for k in valcols:
            combined_cols.append(k)

        # remaining validation done by the group_by_key
        return self.group_by_key(grouping_cols, combined_cols, __arg_max_grouping_func__, suffix = suffix, collapse = collapse)

    # TODO: this use_string_datatype is temporary and needs to be replaced with better design.
    def aggregate(self, grouping_col_or_cols, agg_cols, agg_funcs, collapse = True, precision = 6, use_rolling = False, use_string_datatype = False, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("aggregate: empty tsv")

        # get matching columns
        grouping_cols = self.__get_matching_cols__(grouping_col_or_cols)

        # define rolling functions
        rolling_agg_funcs_map = {"sum": get_rolling_func_update_sum, "min": get_rolling_func_update_min, "max": get_rolling_func_update_max, "mean": get_rolling_func_update_mean,
            "len": get_rolling_func_update_len}

        # validation on number of grouping cols
        if (len(grouping_cols) == 0 or len(agg_cols) == 0):
            raise Exception("No input columns:", grouping_cols, agg_cols, suffix)

        # validation on number of agg funcs
        if (len(agg_cols) != len(agg_funcs)):
            raise Exception("Aggregate functions are not of correct size")

        # validation
        indexes = self.__get_col_indexes__(grouping_cols)

        # check for column to be aggregated
        new_cols = []
        for i in range(len(agg_cols)):
            agg_col = agg_cols[i]
            if (agg_col not in self.header_map.keys()):
                raise Exception("Column not found: ", str(agg_col) + ", header:", str(self.header_fields))

            new_cols.append(agg_col + ":" + get_func_name(agg_funcs[i]))

        # take the indexes
        agg_col_indexes = []
        rolling_agg_col_indexes_map = {}
        rolling_agg_funcs = []

        # for each agg col, add the index
        for i in range(len(agg_cols)):
            agg_col = agg_cols[i]
            agg_index = self.header_map[agg_col]
            agg_col_indexes.append(agg_index)

            # prepare map of indexes of rolling_agg functions
            if (get_func_name(agg_funcs[i]) in rolling_agg_funcs_map.keys()):
                rolling_agg_col_indexes_map[i] = 1
                rolling_agg_funcs.append(rolling_agg_funcs_map[get_func_name(agg_funcs[i])])

        # create a map to store array of values
        value_map_arr = [{} for i in range(len(agg_col_indexes))]
        # TODO: This rolling aggregation needs to be removed
        # rolling_value_map_arr = [{} for i in range(len(agg_col_indexes))]

        # iterate over the data
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("aggregate: [1/2] building groups", inherit_message, counter, len(self.data))

            # process data
            fields = line.split("\t")

            # new col values
            col_values = []
            for i in indexes:
                col_values.append(fields[i])
            cols_key = "\t".join(col_values)

            # for each possible aggregation, do this
            for j in range(len(agg_col_indexes)):
                if (cols_key not in value_map_arr[j].keys()):
                    value_map_arr[j][cols_key] = []

                # check for rolling functions
                if (use_rolling and j in rolling_agg_col_indexes_map.keys()):
                    if (len(value_map_arr[j][cols_key]) == 0):
                        value_map_arr[j][cols_key] = get_rolling_func_init(get_func_name(agg_funcs[j]))
                    #get_rolling_func_update(value_map_arr[j][cols_key], float(fields[agg_col_indexes[j]]), get_func_name(agg_funcs[j]))
                    rolling_agg_funcs[j](value_map_arr[j][cols_key], float(fields[agg_col_indexes[j]]))
                else:
                    # TODO: this is a hack on datatype
                    if (use_string_datatype == False):
                        try:
                            value_map_arr[j][cols_key].append(float(fields[agg_col_indexes[j]]))
                        except ValueError:
                            value_map_arr[j][cols_key].append(fields[agg_col_indexes[j]])
                    else:
                        value_map_arr[j][cols_key].append(str(fields[agg_col_indexes[j]]))

        # compute the aggregation
        value_func_map_arr = [{} for i in range(len(agg_col_indexes))]

        # for each possible index, do aggregation
        for j in range(len(agg_col_indexes)):
            for k, vs in value_map_arr[j].items():
                # check for rolling functions
                if (use_rolling and j in rolling_agg_col_indexes_map.keys()):
                    value_func_map_arr[j][k] = get_rolling_func_closing(vs, get_func_name(agg_funcs[j]))
                else:
                    value_func_map_arr[j][k] = agg_funcs[j](vs)

        # create new header and data
        new_header = "\t".join(utils.merge_arrays([self.header_fields, new_cols]))
        new_data = []

        # for each output line, attach the new aggregate value
        counter = 0
        cols_key_map = {}
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("aggregate: [2/2] calling function", inherit_message, counter, len(self.data))

            # data processing
            fields = line.split("\t")
            col_values = []
            for i in indexes:
                col_values.append(fields[i])
            cols_key = "\t".join(col_values)

            # append the aggregated values
            agg_values = []
            for j in range(len(agg_col_indexes)):
                agg_values.append(str(value_func_map_arr[j][cols_key]))

            # check for different flags
            if (collapse == False or cols_key not in cols_key_map.keys()):
                new_line = "\t".join(utils.merge_arrays([fields, agg_values]))
                new_data.append(new_line)
                cols_key_map[cols_key] = 1

        # return
        if (collapse == True):
            # uniq cols
            uniq_cols = []
            for col in grouping_cols:
                uniq_cols.append(col)
            for new_col in new_cols:
                uniq_cols.append(new_col)
            return TSV(new_header, new_data).to_numeric(new_cols, precision, inherit_message = inherit_message).select(uniq_cols)
        else:
            return TSV(new_header, new_data).to_numeric(new_cols, precision, inherit_message = inherit_message)

    def filter(self, cols, func, include_cond = True, ignore_if_missing = False, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("filter: empty tsv", ignore_if_missing)
            return self

        # TODO: Filter should not use regex. Need to add warning as the order of fields matter
        cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)
        indexes = self.__get_col_indexes__(cols)

        # count the number of columns
        num_cols = len(cols)

        # check if there were any matching columns
        if (num_cols == 0):
            utils.raise_exception_or_warn("filter: no matching cols", ignore_if_missing)
            return self

        # new data
        new_data = []
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("filter: [1/1] calling function", inherit_message, counter, len(self.data))

            fields = line.split("\t")
            col_values = []
            for index in indexes:
                col_values.append(fields[index])

            # switch case for different number of inputs
            if (num_cols == 1):
                result = func(col_values[0])
            elif (num_cols == 2):
                result = func(col_values[0], col_values[1])
            elif (num_cols == 3):
                result = func(col_values[0], col_values[1], col_values[2])
            elif (num_cols == 4):
                result = func(col_values[0], col_values[1], col_values[2], col_values[3])
            elif (num_cols == 5):
                result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4])
            elif (num_cols == 6):
                result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5])
            elif (num_cols == 7):
                result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6])
            elif (num_cols == 8):
                result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7])
            elif (num_cols == 9):
                result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8])
            elif (num_cols == 10):
                result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9])
            else:
                raise Exception("Number of columns is not supported beyond 10" + str(cols))

            if (result == include_cond):
                new_data.append(line)

        # return
        return TSV(self.header, new_data)

    def exclude_filter(self, cols, func, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": exclude_filter" if (inherit_message != "") else "exclude_filter"
        return self.filter(cols, func, include_cond = False, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def transform(self, cols, func, new_col_or_cols, use_array_notation = False, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("transform: empty tsv")

        # resolve to matching_cols
        matching_cols = self.__get_matching_cols__(cols)

        # find if the new cols is a single value or array
        if (isinstance(new_col_or_cols, str)):
            new_cols = [new_col_or_cols]
        else:
            new_cols = new_col_or_cols

        # number of new_cols
        num_new_cols = len(new_cols)

        # validation
        if ((utils.is_array_of_string_values(cols) == False and len(matching_cols) != 1) or (utils.is_array_of_string_values(cols) == True and len(matching_cols) != len(cols))):
            raise Exception("transform api doesnt support regex style cols array as the order of columns matter:", cols, matching_cols)

        # validation
        for col in matching_cols:
            if (col not in self.header_map.keys()):
                raise Exception("Column not found:", str(col), str(self.header_fields))

        # new col validation
        for new_col in new_cols:
            if (new_col in self.header_fields):
                raise Exception("New column already exists:", new_col, str(self.header_fields))

        # get the indexes
        num_cols = len(matching_cols)
        indexes = []
        for col in matching_cols:
            indexes.append(self.header_map[col])

        # create new header and data
        new_header = "\t".join(utils.merge_arrays([self.header_fields, new_cols]))
        new_data = []
        counter = 0

        # iterate over data
        for line in self.data:
            counter = counter + 1
            utils.report_progress("transform: [1/1] calling function", inherit_message, counter, len(self.data))

            # get fields
            fields = line.split("\t")
            col_values = []

            # get the fields in cols
            for index in indexes:
                col_values.append(fields[index])

            # check which notation is used to do the function call
            if (use_array_notation == False):
                if (num_cols == 1):
                    # TODO: this if condition is not able to do error check when number of output columns doesnt match number of input cols
                    result = func(col_values[0])
                elif (num_cols == 2):
                    result = func(col_values[0], col_values[1])
                elif (num_cols == 3):
                    result = func(col_values[0], col_values[1], col_values[2])
                elif (num_cols == 4):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3])
                elif (num_cols == 5):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4])
                elif (num_cols == 6):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5])
                elif (num_cols == 7):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6])
                elif (num_cols == 8):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7])
                elif (num_cols == 9):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8])
                elif (num_cols == 10):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9])
                elif (num_cols == 11):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10])
                elif (num_cols == 12):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11])
                elif (num_cols == 13):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11], col_values[12])
                elif (num_cols == 14):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11], col_values[12], col_values[13])
                elif (num_cols == 15):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11], col_values[12], col_values[13], col_values[15])
                else:
                    raise Exception("Number of columns is not supported beyond 15. Probably try to use use_array_notation approach:" + str(cols))
            else:
                result = func(col_values)

            # create new line and append to data. Do validation
            result_arr = []
            if (use_array_notation == False):
                if (num_new_cols == 1):
                    if (isinstance(result, list)):
                        result_arr.append(str(result[0]))
                    else:
                        result_arr.append(str(result))
                if (num_new_cols >= 2):
                    result_arr.append(str(result[0]))
                    result_arr.append(str(result[1]))
                if (num_new_cols >= 3):
                    result_arr.append(str(result[2]))
                if (num_new_cols >= 4):
                    result_arr.append(str(result[3]))
                if (num_new_cols >= 5):
                    result_arr.append(str(result[4]))
                if (num_new_cols >= 6):
                    result_arr.append(str(result[5]))
                if (num_new_cols >= 7):
                    result_arr.append(str(result[6]))
                if (num_new_cols >= 8):
                    result_arr.append(str(result[7]))
                if (num_new_cols >= 9):
                    result_arr.append(str(result[8]))
                if (num_new_cols >= 10):
                    result_arr.append(str(result[9]))
                if (num_new_cols >= 11):
                    raise Exception("Number of new columns is not supported beyond 10. Probably try to use use_array_notation approach:" + str(new_cols))
            else:
                # check how many columns to expect.
                if (num_new_cols == 1):
                    if (isinstance(result, str)):
                        result_arr.append(str(result))
                    elif (isinstance(result, list)):
                        result_arr.append(str(result[0]))
                    else:
                        result_arr.append(str(result))
                else:
                    if (len(result) != num_new_cols):
                        raise Exception("Invalid number of fields in the result array. Expecting: {}, Got: {}, result: {}, new_cols: {}".format(num_new_cols, len(result), result, new_cols))
                    for r in result:
                        result_arr.append(r)

            # create new line
            new_line = "\t".join(utils.merge_arrays([fields, result_arr]))
            new_data.append(new_line)

        # return
        return TSV(new_header, new_data)

    def transform_inline(self, cols, func, ignore_if_missing = False, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("transform_inline: empty tsv", ignore_if_missing)
            return self

        # find the matching cols and indexes
        matching_cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)
        indexes = self.__get_col_indexes__(matching_cols)

        # check if there were any matching columns
        if (len(matching_cols) == 0):
            utils.raise_exception_or_warn("transform_inline: no matching columns", ignore_if_missing)
            return self

        # print which columns are going to be transformed
        if (len(matching_cols) != len(cols) and len(matching_cols) != 1):
            utils.debug("transform_inline: list of columns that will be transformed: {}".format(str(matching_cols)))

        # create new data
        new_data = []
        counter = 0
        for line in self.data:
            counter = counter + 1
            utils.report_progress("transform_inline: [1/1] calling function", inherit_message, counter, len(self.data))

            fields = line.split("\t")
            new_fields = []
            for i in range(len(fields)):
                if (i in indexes):
                    new_fields.append(str(func(fields[i])))
                else:
                    new_fields.append(str(fields[i]))

            new_data.append("\t".join(new_fields))

        return TSV(self.header, new_data)

    def rename(self, col, new_col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("rename: empty tsv")

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        # validation
        if (new_col in self.header_map.keys()):
            raise Exception("New Column already exists:", str(new_col), str(self.header_fields))

        index = self.header_map[col]
        header_fields2 = []
        for h in self.header_fields:
            if (h == col):
                header_fields2.append(new_col)
            else:
                header_fields2.append(h)

        new_header = "\t".join(header_fields2)
        return TSV(new_header, self.data)

    def get_header(self):
        return self.header

    def get_data(self):
        return self.data

    def get_header_map(self):
        return self.header_map

    def num_rows(self):
        return len(self.data)

    def num_cols(self):
        return len(self.header_map)

    def get_size_in_bytes(self):
        utils.warn("Please use size_in_bytes() instead")
        return self.size_in_bytes()

    def size_in_bytes(self):
        total = len(self.header)
        for line in self.data:
            total = total + len(line)
        return total

    def size_in_mb(self):
        return int(self.size_in_bytes() / 1e6)

    def size_in_gb(self):
        return int(self.size_in_bytes() / 1e9)

    def get_header_fields(self):
        return self.header_fields

    def get_columns(self):
        return self.get_header_fields()

    def columns(self):
        utils.warn("Deprecated. Use get_columns() instead")
        return self.get_columns()

    def get_column_index(self, col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("get_column_index: empty tsv")

        # validation
        if (col not in self.get_columns()):
            raise Exception("Column not found: {}, {}".format(col, self.get_columns()))

        # get index
        header_map = self.get_header_map()
        return header_map[col]

    def export_to_maps(self):
        utils.warn("Please use to_maps()")
        return self.to_maps()

    def to_maps(self):
        mps = []
        for line in self.data:
            fields = line.split("\t")
            mp = {}
            for i in range(len(self.header_fields)):
                mp[self.header_fields[i]] = str(fields[i])
            mps.append(mp)

        return mps

    def __convert_to_numeric__(self, x, precision = 6):
        try:
            if (int(float(x)) == float(x)):
                return str(int(float(x)))
            else:
                precision_str = "{:." + str(precision) + "f}"
                return precision_str.format(float(x))
        except ValueError:
            return str(x)

    # TODO
    def to_numeric(self, cols, precision = 6, inherit_message = ""):
        inherit_message2 = inherit_message + ": to_numeric" if (len(inherit_message) > 0) else "to_numeric"
        return self.transform_inline(cols, lambda x: self.__convert_to_numeric__(x, precision), inherit_message = inherit_message2)

    def add_seq_num(self, new_col, start = 1, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("add_seq_num: empty tsv")
            return self

        # validation
        if (new_col in self.header_map.keys()):
            raise Exception("Output column name already exists:", new_col, self.header_fields)

        # create new header
        new_header = new_col + "\t" + self.header

        # create new data
        new_data = []
        counter = start - 1 
        for line in self.data:
            counter = counter + 1
            utils.report_progress("add_seq_num: [1/1] adding new column", inherit_message, counter, len(self.data))
            new_data.append(str(counter) + "\t" + line)

        # return
        return TSV(new_header, new_data)

    def show_transpose(self, n = 1, max_col_width = None, title = None):
        # check empty
        if (self.has_empty_header()):
            return self

        # validation and doing min
        if (self.num_rows() < n):
            n = self.num_rows()

        # max width of screen to determine per column width
        if (max_col_width is None):
            max_width = 180
            max_col_width = int(max_width / (n + 1))

        # print
        self.transpose(n).show(n = self.num_cols(), max_col_width = max_col_width, title = title)

        # return self
        return self

    def show(self, n = 100, max_col_width = 40, title = None):
        # check empty
        if (self.has_empty_header()):
            return self

        self.take(n).__show_topn__(max_col_width, title)
        # return the original tsv
        return self

    def __show_topn__(self, max_col_width, title):
        spaces = " ".join([""]*max_col_width)

        # gather data about width of columns
        col_widths = {}
        is_numeric_type_map = {}

        # determine which columns are numeric type
        for k in self.header_map.keys():
            col_widths[k] = min(len(k), max_col_width)
            is_numeric_type_map[k] = True

        # determine width
        for line in self.data:
            fields = line.split("\t")
            for i in range(len(fields)):
                k = self.header_index_map[i]
                value = fields[i]
                col_widths[k] = min(max_col_width, max(col_widths[k], len(str(value))))
                try:
                    vfloat = float(value)
                except ValueError:
                    is_numeric_type_map[k] = False

        # combine header and lines
        all_data = [self.header]
        for line in self.data:
            all_data.append(line)

        # print label
        if (title is not None):
            print("=============================================================================================================================================")
            print(title)
            print("=============================================================================================================================================")

        # iterate and print. +1 for header
        for i in range(len(all_data)):
            line = all_data[i]
            fields = line.split("\t")
            row = []
            for j in range(len(fields)):
                col_width = col_widths[self.header_index_map[j]]
                value = str(fields[j])
                if (len(value) > col_width):
                    value = value[0:col_width]
                elif (len(value) < col_width):
                    if (j > 0 and is_numeric_type_map[self.header_index_map[j]] == True):
                        value = spaces[0:col_width - len(value)] + value
                    else:
                        value = value + spaces[0:col_width - len(value)]

                row.append(str(value))
            print("\t".join(row))

        if (title is not None):
            print("=============================================================================================================================================")

        # return self
        return self

    def col_as_array(self, col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("col_as_array: empty tsv")

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        index = self.header_map[col]
        ret_values = []
        for line in self.data:
            fields = line.split("\t")
            ret_values.append(str(fields[index]))

        return ret_values

    def col_as_float_array(self, col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("col_as_float_array: empty tsv")

        values = self.col_as_array(col)
        float_values = [float(v) for v in values]
        return float_values

    def col_as_int_array(self, col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("col_as_int_array: empty tsv")

        values = self.col_as_float_array(col)
        numeric_values = [int(v) for v in values]
        return numeric_values

    def col_as_array_uniq(self, col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("col_as_array_uniq: empty tsv")

        values = self.col_as_array(col)
        return list(dict.fromkeys(values))

    # this method returns hashmap of key->map[k:v]
    # TODO: keys should be changed to single column
    def cols_as_map(self, key_cols, value_cols):
        # check empty
        if (self.has_empty_header()):
            raise Exception("cols_as_map: empty tsv")

        # warn
        utils.warn("This api has changed from prev implementation")

        # validation
        key_cols = self.__get_matching_cols__(key_cols)

        # check for all columns in the value part
        value_cols = self.__get_matching_cols__(value_cols)

        # create map
        mp = {}
        for line in self.data:
            fields = line.split("\t")
            # get the key
            keys = []
            for key_col in key_cols:
                key = fields[self.header_map[key_col]]
                keys.append(key)
            keys_tuple = self.__expand_to_tuple__(keys)

            # check for non duplicate keys
            if (keys_tuple in mp.keys()):
                raise Exception("keys is not unique:", keys)

            values = []
            for value_col in value_cols:
                value = fields[self.header_map[value_col]]
                values.append(str(value))
            values_tuple = self.__expand_to_tuple__(values)

            # store value in hashmap
            mp[keys_tuple] = values_tuple

        # return
        return mp

    def __sort_helper__(self, line, indexes, all_numeric):
        values = []
        fields = line.split("\t")
        for i in indexes:
            if (all_numeric == True):
                values.append(float(fields[i]))
            else:
                values.append(fields[i])

        return tuple(values)

    def sort(self, cols = None, reverse = False, reorder = False, all_numeric = None, ignore_if_missing = False):
        # check empty
        if (self.has_empty_header() and cols is None):
            utils.raise_exception_or_warn("sort: empty tsv", ignore_if_missing)
            return self

        # if nothing is specified sort on all columns
        if (cols is None):
            cols = self.get_header_fields()

        # find the matching cols and indexes
        matching_cols = self.__get_matching_cols__(cols)
        indexes = self.__get_col_indexes__(matching_cols)

        # check if there were any matching cols
        if (len(matching_cols) == 0):
            utils.raise_exception_or_warn("sort: no matching cols found.", ignore_if_missing)
            return self

        # check if all are numeric or not
        if (all_numeric is None):
            has_alpha = False
            for col in matching_cols:
                # check for data type for doing automatic numeric or string sorting
                if (self.__has_all_float_values__(col) == False):
                    has_alpha = True
                    break

            # if any string column was found, then use string sorting else numeric
            if (has_alpha == True):
                all_numeric = False
            else:
                all_numeric = True

        # sort
        new_data = sorted(self.data, key = lambda line: self.__sort_helper__(line, indexes, all_numeric = all_numeric), reverse = reverse)

        # check if need to reorder the fields
        if (reorder == True):
            return TSV(self.header, new_data).reorder(matching_cols, inherit_message = "sort")
        else:
            return TSV(self.header, new_data)

    def reverse_sort(self, cols = None, reorder = False, all_numeric = None, ignore_if_missing = False):
        return self.sort(cols = cols, reverse = True, reorder = reorder, all_numeric = all_numeric, ignore_if_missing = ignore_if_missing)

    # reorder the specific columns
    def reorder(self, cols, use_existing_order = True, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            if (cols is None):
                utils.warn("reorder: empty tsv")
                return self
            else:
                raise Exception("reorder: empty tsv")

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(cols)
        indexes = self.__get_col_indexes__(matching_cols)

        # do a full reorder if asked
        if (use_existing_order == False):
            # get the non matching cols
            non_matching_cols = []
            for c in self.get_header_fields():
                if (c not in matching_cols):
                    non_matching_cols.append(c)

            # all cols
            all_cols = []
            for c in matching_cols:
                all_cols.apppend(c)
            for c in non_matching_cols:
                all_cols.apppend(c)

            # return
            return self.select(all_cols).reorder(cols, use_existing_order = False, inherit_message = inherit_message)

        # create a map of columns that match the criteria
        new_header_fields = []

        # append all the matching columns
        for h in self.header_fields:
            if (h in matching_cols):
                new_header_fields.append(h)

        # append all the remaining columns
        for h in self.header_fields:
            if (h not in matching_cols):
                new_header_fields.append(h)

        # pass on the message
        inherit_message2 = inherit_message + ": reorder" if (len(inherit_message) > 0) else "reorder"
        return self.select(new_header_fields, inherit_message = inherit_message2)

    def reorder_reverse(self, cols, inherit_message = ""):
        utils.warn("Please use reverse_reorder instead")
        return self.reverse_reorder(cols, inherit_message)

    # reorder for pushing the columns to the end
    def reverse_reorder(self, cols, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("reorder: empty tsv")

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(cols)

        # generate the list of cols that should be brought to front
        rcols = []
        for h in self.header_fields:
            if (h not in matching_cols):
                rcols.append(h)

        # pass on the message
        inherit_message2 = inherit_message + ": reorder_reverse" if (len(inherit_message) > 0) else "reorder_reverse"
        return self.reorder(rcols, inherit_message = inherit_message2)

    def noop(self, *args, **kwargs):
        return self

    def to_df(self, n = None, infer_data_types = False, no_infer_cols = None):
        # find how many rows to select
        nrows = len(self.data) if (n is None) else n

        # validation
        if (nrows < 0):
            raise Exception("to_df(): n can not be negative: {}".format(nrows))

        # initialize map
        df_map = {}
        for h in self.header_fields:
            df_map[h] = []

        # check if infer data type is true
        float_cols = [] 
        int_cols = []
        if (infer_data_types == True):
            for col in self.get_columns():
                # check for columns that are not supposed to be converted
                if (no_infer_cols is not None and col in no_infer_cols):
                    continue

                # determine the inferred data type. Check for int first as that is more constrained
                if (self.__has_all_int_values__(col)):
                    int_cols.append(col)
                if (self.__has_all_float_values__(col)):
                    float_cols.append(col)

        # iterate over data
        for line in self.data[0:nrows]:
            fields = line.split("\t")
            for i in range(len(fields)):
                col = self.get_columns()[i]
                value = fields[i]

                # check if a different data type is needed
                if (infer_data_types == True):
                    if (col in float_cols):
                        value = float(value)
                    elif (col in int_cols):
                        value = int(value)
                    else:
                        value = str(value)
                else:
                    value = str(value)

                # assign the value
                df_map[self.header_index_map[i]].append(value)

        # return
        return pd.DataFrame(df_map)

    def export_to_df(self, n = -1):
        utils.warn("Deprecated. Use to_df()")
        return self.to_df(n)

    def to_json_records(self, new_col = "json"):
        new_header = new_col
        new_data = []

        # new col validation
        if (new_col in self.header_map.keys()):
            raise Exception("New column already exists:", new_col, str(self.header_fields))

        # iterate
        for line in self.data:
            fields = line.split("\t")
            mp = {}
            for i in range(len(self.header_fields)):
                mp[self.header_fields[i]] = fields[i]
            new_data.append(json.dumps(mp))

        return TSV(new_header, new_data)

    def to_csv(self, comma_replacement = ";"):
        utils.warn("to_csv: This is not a standard csv conversion. The commas are just replaced with another character specified in comma_replacement parameter.")

        # create new data
        new_header = self.header.replace(",", comma_replacement).replace("\t", ",")
        new_data = []
        for line in self.data:
            line = line.replace(",", comma_replacement).replace("\t", ",")
            new_data.append(line)

        # still return as tsv with single column that is special
        return TSV(new_header, new_data)

    def url_encode_inline(self, col_or_cols, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": url_encode_inline" if (len(inherit_message) > 0) else "url_encode_inline"
        return self.transform_inline(col_or_cols, lambda x: utils.url_encode(x), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def url_decode_inline(self, col_or_cols, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": url_decode_inline" if (len(inherit_message) > 0) else "url_decode_inline"
        return self.transform_inline(col_or_cols, lambda x: utils.url_decode(x), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def url_decode_clean_inline(self, col_or_cols, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": url_decode_clean_inline" if (len(inherit_message) > 0) else "url_decode_clean_inline"
        return self.transform_inline(col_or_cols, lambda x: utils.url_decode_clean(x), ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    def url_encode(self, col, newcol):
        return self.transform([col], lambda x: utils.url_encode(x), newcol)

    def url_decode(self, col, newcol):
        return self.transform([col], lambda x: utils.url_decode(x), newcol)

    def union(self, tsv_or_that_arr):
        # check if this is a single element TSV or an array
        if (type(tsv_or_that_arr) == TSV):
            that_arr = [tsv_or_that_arr]
        else:
            that_arr = tsv_or_that_arr

        # boundary condition
        if (len(that_arr) == 0):
            return self

        # check empty
        if (self.has_empty_header()):
            # if there are multiple tsvs
            if (len(that_arr) > 1):
                return self.union(that_arr[0], that_arr[1:])
            else:
                return that_arr[0]

        # validation
        for that in that_arr:
            if (self.get_header() != that.get_header()):
                raise Exception("Headers are not matching for union", self.header_fields, that.header_fields)

        # create new data
        new_data = []
        for line in self.get_data():
            fields = line.split("\t")
            if (len(fields) != len(self.header_fields)):
                raise Exception("Invalid input data. Fields size are not same as header: header: {}, fields: {}".format(self.header_fields, fields))
            new_data.append(line)

        for that in that_arr:
            for line in that.get_data():
                fields = line.split("\t")
                if (len(fields) != len(self.header_fields)):
                    raise Exception("Invalid input data. Fields size are not same as header: header: {}, fields: {}".format(self.header_fields, fields))
                new_data.append(line)

        return TSV(self.header, new_data)

    # this method finds the set difference between this and that. if cols is None, then all columns are taken
    def difference(self, that, cols = None):
        # print some warning for api that is still under development
        utils.warn("difference: to be used where duplicate rows will be removed")

        # check this empty. return empty
        if (self.has_empty_header() or self.num_rows() == 0):
            return self

        # check that empty.  return self
        if (that.is_empty() or that.num_rows() == 0):
            return self

        # define columns
        cols1 = None
        cols2 = None

        # resolve columns
        if (cols is None):
            cols1 = self.get_columns()
            cols2 = that.get_columns()
        else:
            cols1 = self.__get_matching_cols__(cols)
            cols2 = that.__get_matching_cols__(cols)

        # generate key hash
        temp_col = "__difference_col__"
        hash_tsv1= self.generate_key_hash(cols1, temp_col)
        hash_tsv2 = that.generate_key_hash(cols2, temp_col)

        # remove entries from this where the hashes exist in that
        return hash_tsv1 \
            .values_not_in(temp_col, hash_tsv2.col_as_array_uniq(temp_col)) \
            .drop(temp_col)

    def add_const(self, col, value, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            # checking empty value
            if (value == ""):
                utils.warn("add_const: empty header and empty data. extending just the header")
                return new_with_cols([col])
            else:
                raise Exception("add_const: empty header but non empty data: {}".format(value))

        # return
        inherit_message2 = inherit_message + ": add_const" if (len(inherit_message) > 0) else "add_const"
        return self.transform([self.header_fields[0]], lambda x: str(value), col, inherit_message = inherit_message2)

    def add_const_if_missing(self, col, value, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            # checking empty value
            if (value == ""):
                utils.warn("add_const_if_missing: empty tsv and empty value. extending just the header")
                return new_with_cols([col])
            else:
                raise Exception("add_const_if_missing: empty tsv but non empty value")

        # check for presence
        if (col in self.header_fields):
            return self
        else:
            inherit_message2 = inherit_message + ": add_const_if_missing" if (len(inherit_message) > 0) else "add_const_if_missing"
            return self.add_const(col, value, inherit_message = inherit_message2)

    def add_empty_cols_if_missing(self, cols, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            return new_with_cols(cols)

        # add only if missing
        missing_cols = []
        for col in cols:
            if (col not in self.get_header_fields()):
                missing_cols.append(col)

        # check for no missing cols
        if (len(missing_cols) == 0):
            return self

        # create new header fields
        new_header_fields = utils.merge_arrays([self.get_header_fields(), missing_cols])

        # check no data
        if (self.num_rows() == 0):
            return new_with_cols(new_header_fields)

        # create new header and empty row for missing fields
        new_header = "\t".join(new_header_fields)
        empty_row = "\t".join(list(["" for c in missing_cols]))
        new_data = []

        # iterate and add
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("add_empty_cols_if_missing: [1/1] calling function", inherit_message, counter, len(self.data))

            # create new line
            new_line = "\t".join([line, empty_row])
            new_data.append(new_line)

        # return
        return TSV(new_header, new_data)

    def add_row(self, row_fields):
        # check empty
        if (self.has_empty_header()):
            raise Exception("add_row: empty tsv")

        # validation
        if (len(row_fields) != self.num_cols()):
            raise Exception("Number of fields is not matching with number of columns: {} != {}".format(len(row_fields), self.num_cols()))

        # create new row
        new_line = "\t".join(row_fields)

        # remember to do deep copy
        new_data = []
        for line in self.data:
            new_data.append(line)
        new_data.append(new_line)

        # return
        return TSV(self.header, new_data)

    def add_map_as_row(self, mp, default_val = None):
        # check empty
        if (self.has_empty_header()):
            raise Exception("add_map_as_row: empty tsv")

        # validation
        for k in mp.keys():
            if (k not in self.header_fields):
                raise Exception("Column not in existing data: {}, {}".format(k, str(self.header_fields)))

        # check for default values
        for h in self.header_fields:
            if (h not in mp.keys()):
                if (default_val is None):
                    raise Exception("Column not present in map and default value is not defined: {}. Try using default_val".format(h))

        # add the map as new row
        new_fields = []
        for h in self.header_fields:
            if (h in mp.keys()):
                # append the value
                new_fields.append(utils.strip_spl_white_spaces(mp[h]))
            else:
                # append default value
                new_fields.append(default_val)

        # create new row
        return self.add_row(new_fields)

    def assign_value(self, col, value, inherit_message = ""):
        inherit_message2 = inherit_message + ": assign_value" if (len(inherit_message) > 0) else "assign_value"
        return self.transform_inline(col, lambda x: value, inherit_message = inherit_message2)

    def concat_as_cols(self, that):
        # check empty
        if (self.has_empty_header()):
            utils.warn("concat_as_cols: empty tsv")
            return that

        # validation
        if (self.num_rows() != that.num_rows()):
            raise Exception("Mismatch in number of rows:", self.num_rows(), that.num_rows())

        # check for complete disjoint set of columns
        for h in self.header_map.keys():
            if (h in that.header_map.keys()):
                raise Exception("The columns for doing concat need to be completely separate:", h, self.header_fields)

        # create new header
        new_header_fields = []
        for h in self.header.split("\t"):
            new_header_fields.append(h)
        for h in that.header.split("\t"):
            new_header_fields.append(h)

        new_header = "\t".join(new_header_fields)

        # create new data
        new_data = []
        for i in range(len(self.data)):
            line1 = self.data[i]
            line2 = that.data[i]

            new_data.append(line1 + "\t" + line2)

        return TSV(new_header, new_data)

    def add_col_prefix(self, cols, prefix):
        utils.warn("Deprecated: Use add_prefix instead")
        return self.add_prefix(self, prefix, cols)

    def remove_suffix(self, suffix, ignore_if_missing = False):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("remove_suffix: empty tsv", ignore_if_missing)
            return self

        # create a map
        mp = {}

        # validation
        if (suffix.startswith(":") == False):
            suffix = ":" + suffix

        # check for matching cols
        for c in self.header_fields:
            if (c.endswith(suffix)):
                new_col =  c[0:-len(suffix)]
                if (new_col in self.header_fields or len(new_col) == 0):
                    utils.warn("remove_suffix: Duplicate names found. Ignoring removal of prefix for col: {} to new_col: {}".format(c, new_col))
                else:
                    mp[c] = new_col

        # validation
        if (len(mp) == 0):
            utils.raise_exception_or_warn("suffix didnt match any of the columns: {}, {}".format(suffix, str(self.get_header_fields())), ignore_if_missing)

        new_header = "\t".join(list([h if (h not in mp.keys()) else mp[h] for h in self.header_fields]))
        return TSV(new_header, self.data)

    def add_prefix(self, prefix, cols = None, ignore_if_missing = False):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("add_prefix: empty tsv", ignore_if_missing)
            return self

        # by default all columns are renamed
        if (cols is None):
            cols = self.header_fields

        # resolve columns
        cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)

        # create new header_fields
        new_header_fields = []

        # iterate and set the new name
        for h in self.header_fields:
            if (h in cols):
                new_header_fields.append(prefix + ":" + h)
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def add_suffix(self, suffix, cols = None, ignore_if_missing = False):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("add_suffix: empty tsv", ignore_if_missing)
            return self

        # by default all columns are renamed
        if (cols is None):
            cols = self.header_fields

        # resolve columns
        cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)

        # create new header_fields
        new_header_fields = []

        # iterate and set the new name
        for h in self.header_fields:
            if (h in cols):
                new_header_fields.append(h + ":" + suffix)
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def rename_prefix(self, old_prefix, new_prefix, cols = None, ignore_if_missing = False):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("rename_prefix: empty tsv", ignore_if_missing)
            return self

        # either selective columns can be renamed or all matching ones
        if (cols is None):
            # use the prefix patterns for determing cols
            cols = "{}:.*".format(old_prefix)

        # resolve
        cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)

        # create new header_fields
        new_header_fields = []

        # iterate and set the new name
        for h in self.header_fields:
            if (h in cols):
                new_header_fields.append(new_prefix + h[len(old_prefix):])
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def rename_suffix(self, old_suffix, new_suffix, cols = None, ignore_if_missing = False):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("rename_suffix: empty tsv", ignore_if_missing)
            return self

        # either selective columns can be renamed or all matching ones
        if (cols is None):
            # use the prefix patterns for determing cols
            cols = ".*:{}".format(old_suffix)

        # resolve
        cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)

        # create new header_fields
        new_header_fields = []

        # iterate and set the new name
        for h in self.header_fields:
            if (h in cols):
                new_header_fields.append(h[0:-1*len(old_suffix) + new_suffix])
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def remove_prefix(self, prefix, ignore_if_missing = False):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("remove_prefix: empty tsv", ignore_if_missing)
            return self

        # create a map
        mp = {}

        # validation
        if (prefix.endswith(":") == False):
            prefix = prefix + ":"

        # check for matching cols
        for c in self.header_fields:
            if (c.startswith(prefix)):
                new_col =  c[len(prefix):]
                if (new_col in self.header_fields or len(new_col) == 0):
                    raise Exception("Duplicate names. Cant do the prefix:", c, new_col, str(self.header_fields))
                mp[c] = new_col

        # validation
        if (len(mp) == 0):
            utils.raise_exception_or_warn("prefix didnt match any of the columns: {}, {}".format(prefix, str(self.get_header_fields())), ignore_if_missing)

        new_header = "\t".join(list([h if (h not in mp.keys()) else mp[h] for h in self.header_fields]))
        return TSV(new_header, self.data)

    def sample(self, sampling_ratio, seed = 0, with_replacement = False, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample: empty tsv")
            return self

        # TODO
        if (with_replacement == True):
            raise Exception("sampling with replacement not implemented yet.")

        # set seed
        # this random number is only for basic sampling and not for doing anything sensitive.
        random.seed(seed)  # nosec

        # create variables for data
        new_data = []
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("sample: [1/1] calling function", inherit_message, counter, len(self.data))

            # this random number is only for basic sampling and not for doing anything sensitive.
            if (random.random() <= sampling_ratio):  # nosec
                new_data.append(line)

        return TSV(self.header, new_data)

    def sample_without_replacement(self, sampling_ratio, seed = 0, inherit_message = ""):
        inherit_message2 = inherit_message + ": sample_without_replacement" if (inherit_message != "") else "sample_without_replacement"
        return self.sample(sampling_ratio, seed, with_replacement = False, inherit_message = inherit_message2)

    def sample_with_replacement(self, sampling_ratio, seed = 0, inherit_message = ""):
        inherit_message2 = inherit_message + ": sample_with_replacement" if (inherit_message != "") else "sample_with_replacement"
        return self.sample(sampling_ratio, seed, with_replacement = True, inherit_message = inherit_message2)

    def sample_rows(self, n, seed = 0, inherit_message = ""):
        utils.warn("Please use sample_n")
        inherit_message2 = inherit_message + ": sample_rows" if (inherit_message != "") else "sample_rows"
        return self.sample_n(n, seed, inherit_message = inherit_message2)

    def sample_n(self, n, seed = 0, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_n: empty tsv")
            return self

        # validation
        if (n < 1):
            raise Exception("n cant be negative or less than 1:", n)

        # set seed
        random.seed(seed)
        n = min(int(n), self.num_rows())

        # sample and return. the debug message is not in standard form, but its fine.
        utils.report_progress("sample_n: [1/1] calling function", inherit_message, len(self.data), len(self.data))
        return TSV(self.header, random.sample(self.data, n))

    def cap_min_inline(self, col, value):
        inherit_message2 = inherit_message + ": cap_min_inline" if (len(inherit_message) > 0) else "cap_min_inline"
        return self.transform_inline([col], lambda x: str(x) if (float(value) < float(x)) else str(value), inherit_message = inherit_message2)

    def cap_max_inline(self, col, value):
        inherit_message2 = inherit_message + ": cap_max_inline" if (len(inherit_message) > 0) else "cap_max_inline"
        return self.transform_inline(col, lambda x: str(value) if (float(value) < float(x)) else str(x), inherit_message = inherit_message2)

    def cap_min(self, col, value, newcol):
        inherit_message2 = inherit_message + ": cap_min" if (len(inherit_message) > 0) else "cap_min"
        return self.transform_inline(col, lambda x: str(x) if (float(value) < float(x)) else str(value), newcol, inherit_message = inherit_message2)

    def cap_max(self, col, value, newcol):
        inherit_message2 = inherit_message + ": cap_max" if (len(inherit_message) > 0) else "cap_max"
        return self.transform([col], lambda x: str(value) if (float(value) < float(x)) else str(x), newcol, inherit_message = inherit_message2)

    def copy(self, col, newcol, inherit_message = ""):
        inherit_message2 = inherit_message + ": copy" if (len(inherit_message) > 0) else "copy"
        return self.transform([col], lambda x: x, newcol, inherit_message = inherit_message2)

    # sampling method to do class rebalancing where the class is defined by a specific col-value. As best practice,
    # the sampling ratios should be determined externally.
    def sample_class(self, col, col_value, sampling_ratio, seed = 0, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_class: empty tsv")
            return self

        # Validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        # cap the sampling ratio to 1
        if (sampling_ratio > 1):
            sampling_ratio = 1.0

        # set the seed
        random.seed(seed)

        # resample
        new_data = []
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("sample_class: [1/1] calling function", inherit_message, counter, len(self.data))

            # get fields
            fields = line.split("\t")
            cv = fields[self.header_map[col]]

            # check if we need to resample this column value
            if (cv == col_value):
                # this random number is only for basic sampling and not for doing anything sensitive.
                if (random.random() <= sampling_ratio):  # nosec
                    new_data.append(line)
            else:
                new_data.append(line)

        # return
        return TSV(self.header, new_data)

    def __sample_group_by_col_value_agg_func__(self, value, sampling_ratio, seed, use_numeric):
        # set the seed outside
        random.seed(seed)

        def __sample_group_by_col_value_agg_func_inner__(vs):
            # validation. all vs values should be same
            if (len(vs) == 0 or len(set(vs)) > 1):
                raise Exception("sample_group api requires all rows for the same group to have the same value")

            # do this ugly numerical conversion
            vs0 = vs[0]
            if (use_numeric):
                vs0 = float(vs0)
                value0 = float(value)
            else:
                vs0 = str(vs0)
                value0 = str(value)

            # check the value.
            # this random number is only for basic sampling and not for doing anything sensitive.
            if (vs0 != value0 or random.random() <= sampling_ratio):  # nosec
                 return "1"
            else:
                 return "0"

        # return
        return __sample_group_by_col_value_agg_func_inner__

    # sampling method where each sample group is restricted by the max values for a specific col-value. Useful for reducing skewness in dataset
    def sample_group_by_col_value(self, grouping_cols, col, col_value, sampling_ratio, seed = 0, use_numeric = False, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_col_value: empty tsv")
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        # check sampling ratio
        if (sampling_ratio < 0 or sampling_ratio > 1):
            raise Exception("Sampling ratio has to be between 0 and 1:", sampling_ratio)

        # group by and apply the sampling on the value. The assumption is that all rows in the same group should have the same col_value
        inherit_message2 = inherit_message + ": sample_group_by_col_value" if (len(inherit_message) > 0) else "sample_group_by_col_value"
        agg_result = self.aggregate(grouping_cols, [col], [self.__sample_group_by_col_value_agg_func__(col_value, sampling_ratio, seed, use_numeric)], collapse = False, inherit_message = inherit_message2 + ": [1/3]") \
            .values_in("{}:__sample_group_by_col_value_agg_func_inner__".format(col), ["1"], inherit_message = inherit_message2 + ": [2/3]") \
            .drop("{}:__sample_group_by_col_value_agg_func_inner__".format(col), inherit_message = inherit_message2 + ": [3/3]")

        # return
        return agg_result

    def __sample_group_by_max_uniq_values_uniq_count__(self, vs):
        return len(set(vs))

    # sampling method to take a grouping key, and a column where the number of unique values for column are capped.
    def sample_group_by_max_uniq_values(self, grouping_cols, col, max_uniq_values, seed = 0, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_max_uniq_values: empty tsv")
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        # check grouping cols
        for k in grouping_cols:
            if (k not in self.header_map.keys()):
                raise Exception("Grouping Column not found:", str(k), str(self.header_fields))

        # check max_uniq_values
        if (max_uniq_values <= 0):
            raise Exception("max_uniq_values has to be more than 0:", max_uniq_values)

        # the hashing function is applied on the entire grouping_cols + col
        sample_grouping_cols = [g for g in grouping_cols]
        sample_grouping_cols.append(col)

        # agg result
        inherit_message2 = inherit_message + ": sample_group_by_max_uniq_values" if (len(inherit_message) > 0) else "sample_group_by_max_uniq_values"
        agg_result = self.aggregate(grouping_cols, [col], [self.__sample_group_by_max_uniq_values_uniq_count__], collapse = False, inherit_message = inherit_message2 + ": [1/5]") \
            .transform(["{}:__sample_group_by_max_uniq_values_uniq_count__".format(col)], lambda c: max_uniq_values / float(c) if (float(c) > max_uniq_values) else 1, "{}:__sample_group_by_max_uniq_values_sampling_ratio__".format(col), inherit_message = inherit_message2 + ": [2/5]") \
            .transform(sample_grouping_cols, lambda x: abs(utils.compute_hash("\t".join(x), seed)) / sys.maxsize, "{}:__sample_group_by_max_uniq_values_sampling_key__".format(col), use_array_notation = True, inherit_message = inherit_message2 + ": [3/5]") \
            .filter(["{}:__sample_group_by_max_uniq_values_sampling_key__".format(col), "{}:__sample_group_by_max_uniq_values_sampling_ratio__".format(col)], lambda x, y: float(x) <= float(y), inherit_message = inherit_message2 + ": [4/5]") \
            .drop("{}:__sample_group_by_max_uniq_values_.*".format(col), inherit_message = inherit_message2 + ": [5/5]")

        # return
        return agg_result

    def __sample_group_by_max_uniq_values_per_class_uniq_count__(self, vs):
        return len(set(vs))

    # sampling method to take a grouping key, and a column where the number of unique values for column are capped.
    def sample_group_by_max_uniq_values_per_class(self, grouping_cols, class_col, col, max_uniq_values_map, def_max_uniq_values = None , seed = 0, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_max_uniq_values_per_class: empty tsv")
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))
        if (class_col not in self.header_map.keys()):
            raise Exception("Column not found:", str(class_col), str(self.header_fields))

        # correctly define def_max_uniq_values
        if (def_max_uniq_values is None):
            def_max_uniq_values = self.num_rows()

        # validation on def_max_uniq_values
        if (def_max_uniq_values <= 0):
            raise Exception("max_uniq_values has to be more than 0:", def_max_uniq_values)

        # the hashing function is applied on the entire grouping_cols + col
        sample_grouping_cols = [g for g in grouping_cols]
        # check if class_col is already in grouping_cols
        if (class_col not in grouping_cols):
            sample_grouping_cols.append(class_col)
        else:
            utils.warn("sample_group_by_max_uniq_values_per_class: class_col need not be specified in the grouping cols: {}".format(str(grouping_cols)))
        sample_grouping_cols.append(col)

        # aggregate result
        inherit_message2 = inherit_message + ": sample_group_by_max_uniq_values_per_class" if (len(inherit_message) > 0) else "sample_group_by_max_uniq_values_per_class"
        agg_result = self.aggregate(grouping_cols, [col], "", [self.__sample_group_by_max_uniq_values_per_class_uniq_count__], collapse = False, inherit_message = inherit_message2 + ": [1/6]") \
            .transform([class_col], lambda c: str(max_uniq_values_map[c]) if (c in max_uniq_values_map.keys()) else str(def_max_uniq_values), "{}:__sample_group_by_max_uniq_values_per_class_max_uniq_values__".format(col), inherit_message = inherit_message2 + ": [2/6]") \
            .transform(["{}:__sample_group_by_max_uniq_values_per_class_uniq_count__".format(col), "{}:__sample_group_by_max_uniq_values_per_class_max_uniq_values__".format(col)], lambda c, m: float(m) / float(c) if (float(c) > float(m)) else 1, "{}:__sample_group_by_max_uniq_values_per_class_sampling_ratio__".format(col), inherit_message = inherit_message2 + ": [3/6]") \
            .transform(sample_grouping_cols, lambda x: abs(utils.compute_hash("\t".join(x), seed)) / sys.maxsize, "{}:__sample_group_by_max_uniq_values_per_class_sampling_key__".format(col), use_array_notation = True, inherit_message = inherit_message2 + ": [4/6]") \
            .filter(["{}:__sample_group_by_max_uniq_values_per_class_sampling_key__".format(col), "{}:__sample_group_by_max_uniq_values_per_class_sampling_ratio__".format(col)], lambda x, y: float(x) <= float(y), inherit_message = inherit_message2 + ": [5/6]") \
            .drop("{}:__sample_group_by_max_uniq_values_per_class.*".format(col), inherit_message = inherit_message2 + ": [6/6]")

        # return
        return agg_result

    # random sampling within a group
    def sample_group_by_key(self, grouping_cols, sampling_ratio, seed = 0, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_key: empty tsv")
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # check sampling ratio
        if (sampling_ratio < 0 or sampling_ratio > 1):
            raise Exception("Sampling ratio has to be between 0 and 1:", sampling_ratio)

        # create new data
        new_data = []
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("sample_group_by_key: [1/1] calling function", inherit_message, counter, len(self.data))

            keys = []
            fields = line.split("\t")
            for g in grouping_cols:
                keys.append(fields[self.header_map[g]])

            keys_str = "\t".join(keys)
            sample_key = abs(utils.compute_hash(keys_str, seed)) / sys.maxsize
            if (sample_key <= sampling_ratio):
                new_data.append(line)

        # return
        return TSV(self.header, new_data)

    # sample by taking only n number of unique values for a specific column
    def sample_column_by_max_uniq_values(self, col, max_uniq_values, seed = 0, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_column_by_max_uniq_values: empty tsv")
            return self

        # get unique values
        uniq_values = self.col_as_array_uniq(col)

        # this random number is only for basic sampling and not for doing anything sensitive.
        random.seed(seed)  # nosec
        if (len(uniq_values) > max_uniq_values):
            # this random number is only for basic sampling and not for doing anything sensitive.
            selected_values = random.sample(uniq_values, max_uniq_values)  # nosec`
            inherit_message2 = inherit_message + ": sample_column_by_max_uniq_values" if (inherit_message != "") else "sample_column_by_max_uniq_values"
            return self.values_in(col, selected_values, inherit_message = inherit_message2)
        else:
            utils.warn("sample_column_by_max_uniq_values: max sample size: {} more than number of uniq values: {}".format(max_uniq_values, len(uniq_values)))
            return self

    # create descriptive methods for join
    def left_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, split_threshold = None, inherit_message = ""):
        # check for empty
        if (self.has_empty_header()):
            utils.warn("left_join: empty this tsv")
            return self

        # return
        inherit_message2 = inherit_message + ": left_join" if (inherit_message != "") else "left_join"
        return self.__join__(that, lkeys, rkeys, join_type = "left", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, split_threshold = split_threshold, inherit_message = inherit_message2)

    def right_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, split_threshold = None, inherit_message = ""):
        # check for empty
        if (self.has_empty_header()):
            utils.warn("right_join: empty this tsv")
            return that

        # return
        inherit_message2 = inherit_message + ": left_join" if (inherit_message != "") else "right_join"
        return self.__join__(that, lkeys, rkeys, join_type = "right", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, split_threshold = split_threshold, inherit_message = inherit_message2)

    def inner_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, split_threshold = None, inherit_message = ""):
        # check for empty
        if (self.has_empty_header()):
            raise Exception("inner_join: empty this tsv")

        # return
        inherit_message2 = inherit_message + ": left_join" if (inherit_message != "") else "inner_join"
        return self.__join__(that, lkeys, rkeys, join_type = "inner", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, split_threshold = split_threshold, inherit_message = inherit_message2)

    def outer_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, split_threshold = None, inherit_message = ""):
        # check for empty
        if (self.has_empty_header()):
            utils.warn("outer_join: empty this tsv")
            return that

        # return
        inherit_message2 = inherit_message + ": left_join" if (inherit_message != "") else "outer_join"
        return self.__join__(that, lkeys, rkeys, join_type = "outer", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, split_threshold = split_threshold, inherit_message = inherit_message2)

    def join(self, *args, **kwargs):
        utils.warn("Use the other methods: inner_join, left_join, right_join, outer_join versions of this api and not this one directly")
        return self.__join__(*args, **kwargs)

    # primary join method. Use the other inner, left, right versions and not this directly
    def __join__(self, that, lkeys, rkeys = None, join_type = "inner", lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, split_threshold = None, inherit_message = ""):
        # matching
        lkeys = self.__get_matching_cols__(lkeys)
        rkeys = that.__get_matching_cols__(rkeys) if (rkeys is not None) else lkeys

        # check the lengths
        if (len(lkeys) != len(rkeys)):
            raise Exception("Length mismatch in lkeys and rkeys:", lkeys, rkeys)

        # Check for split_threshold. TODO: Experimental
        if (split_threshold is not None):
            # check if either side is more than split threshold. If yes, then split and merge
            if (self.num_rows() > split_threshold or that.num_rows() > split_threshold):
                # create batches of smaller tsvs
                num_batches = int(math.ceil(max(self.num_rows(), that.num_rows()) / split_threshold))

                # debug
                utils.debug("join: Number of batches: {}".format(num_batches))

                # split left and right sides
                left_batches = self.__split_batches_by_cols__(num_batches, lkeys)
                right_batches = that.__split_batches_by_cols__(num_batches, rkeys)

                # call join on individual batches and then return the merge
                joined_batches = []
                for i in range(num_batches):
                    # debug
                    utils.debug("Calling join on batch: {}, left: {}, right: {}".format(i, left_batches[i].num_rows(), right_batches[i].num_rows()))

                    # call join on the batch
                    joined_batch = left_batches[i].join(right_batches[i], lkeys, rkeys, join_type = join_type, lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map,
                        inherit_message = inherit_message)
                    joined_batches.append(joined_batch)

                # merge
                return merge(joined_batches)

        # create a hashmap of left key values
        lvkeys = {}
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("join: [1/3] building map for left side", inherit_message, counter, len(self.data))

            # parse data
            fields = line.split("\t")
            lvals1 = []
            lvals2 = []

            # create value string for lkey
            for i in range(len(fields)):
                if (self.header_fields[i] in lkeys):
                    lvals1.append(fields[i])
                else:
                    lvals2.append(fields[i])
            lvals1_str = "\t".join(lvals1)
            #lvals2_str = "\t".join(lvals2)

            # left side values need to be unique
            if (lvals1_str in lvkeys.keys()):
                utils.trace("left side values are not unique: lvals1: {}, lvals2: {}, lvkeys[lvals1_str]: {}".format(lvals1, lvals2, lvkeys[lvals1_str]))

            # check if the key already exists, else create an array
            if (lvals1_str not in lvkeys.keys()):
                lvkeys[lvals1_str] = []

            # append the value
            lvkeys[lvals1_str].append(lvals2)

        # create a hashmap of right key values
        rvkeys = {}
        counter = 0
        for line in that.data:
            # report progress
            counter = counter + 1
            utils.report_progress("join: [2/3] building map for right side", inherit_message, counter, len(that.data))

            # parse data
            fields = line.split("\t")
            rvals1 = []
            rvals2 = []

            # create value string for rkey
            for i in range(len(fields)):
                if (that.header_fields[i] in rkeys):
                    rvals1.append(fields[i])
                else:
                    rvals2.append(fields[i])
            rvals1_str = "\t".join(rvals1)
            #rvals2_str = "\t".join(rvals2)

            # right side values are not unique
            if (rvals1_str in rvkeys.keys()):
                utils.trace("right side values are not unique: rvals1: {}, rvals2: {}, rvkeys[rvals1_str]: {}".format(rvals1, rvals2, rvkeys[rvals1_str]))

            # check if the key already exists, else create an array
            if (rvals1_str not in rvkeys.keys()):
                rvkeys[rvals1_str] = []

            # append the value
            rvkeys[rvals1_str].append(rvals2)

        # create a map of combined keys
        common_keys = {}
        for lvkey in lvkeys.keys():
            if (lvkey in rvkeys.keys()):
                common_keys[lvkey] = 1

        # for each type of join, merge the values
        new_header_fields = []

        # create the keys
        for lkey in lkeys:
            new_header_fields.append(lkey)

        # print message for rkeys that are ignored
        for rkey in rkeys:
            if (rkey not in new_header_fields):
                utils.debug("rkey ignored from output: {}".format(rkey))

        # add the left side columns
        for i in range(len(self.header_fields)):
            if (self.header_fields[i] not in lkeys):
                if (lsuffix is not None):
                    new_header_fields.append(self.header_fields[i] + ":" + lsuffix)
                else:
                    new_header_fields.append(self.header_fields[i])

        # add the right side columns
        for i in range(len(that.header_fields)):
            if (that.header_fields[i] not in rkeys):
                if (rsuffix is not None):
                    new_header_fields.append(that.header_fields[i] + ":" + rsuffix)
                else:
                    if (that.header_fields[i] not in new_header_fields):
                        new_header_fields.append(that.header_fields[i])
                    else:
                        raise Exception("Duplicate key names found. Use lsuffix or rsuffix:", that.header_fields[i])

        # construct new_header
        new_header = "\t".join(new_header_fields)

        # define the default lvalues
        default_lvals = []
        for h in self.header_fields:
            if (h not in lkeys):
                if (def_val_map is not None and h in def_val_map.keys()):
                    default_lvals.append(def_val_map[h])
                else:
                    default_lvals.append(default_val)

        # define the default rvalues
        default_rvals = []
        for h in that.header_fields:
            if (h not in rkeys):
                if (def_val_map is not None and h in def_val_map.keys()):
                    default_rvals.append(def_val_map[h])
                else:
                    default_rvals.append(default_val)

        # generate output by doing join
        new_data = []

        # iterate over left side
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("join: [3/3] join the two groups", inherit_message, counter, len(self.data))

            # parse data
            fields = line.split("\t")
            lvals1 = []
            for lkey in lkeys:
                lval = fields[self.header_map[lkey]]
                lvals1.append(lval)
            lvals1_str = "\t".join(lvals1)
            lvals2_arr = lvkeys[lvals1_str]

            rvals2_arr = [default_rvals]
            if (lvals1_str in rvkeys.keys()):
                rvals2_arr = rvkeys[lvals1_str]

            # do a MxN merge of left side values and right side values
            for lvals2 in lvals2_arr:
                for rvals2 in rvals2_arr:
                    # construct the new line
                    new_line = "\t".join(utils.merge_arrays([[lvals1_str], lvals2, rvals2]))

                    # take care of different join types
                    if (join_type == "inner"):
                        if (lvals1_str in common_keys.keys()):
                            new_data.append(new_line)
                    elif (join_type == "left_outer" or join_type == "left"):
                            new_data.append(new_line)
                    elif (join_type == "right_outer" or join_type == "right"):
                        if (lvals1_str in common_keys.keys()):
                            new_data.append(new_line)
                    elif (join_type == "full_outer" or join_type == "outer"):
                        new_data.append(new_line)
                    else:
                        raise Exception("Unknown join type:", join_type)

        # iterate over right side
        for line in that.data:
            fields = line.split("\t")
            rvals1 = []
            for rkey in rkeys:
                rval = fields[that.header_map[rkey]]
                rvals1.append(rval)
            rvals1_str = "\t".join(rvals1)
            rvals2_arr = rvkeys[rvals1_str]

            lvals2_arr = [default_lvals]
            if (rvals1_str in lvkeys.keys()):
                lvals2_arr = lvkeys[rvals1_str]

            # MxN loop for multiple rows on left and right side
            for lvals2 in lvals2_arr:
                for rvals2 in rvals2_arr:
                    # construct the new line
                    new_line = "\t".join(utils.merge_arrays([[rvals1_str], lvals2, rvals2]))

                    # take care of different join types
                    if (join_type == "inner"):
                        pass
                    elif (join_type == "left_outer" or join_type == "left"):
                        pass
                    elif (join_type == "right_outer" or join_type == "right"):
                        if (rvals1_str not in common_keys.keys()):
                            new_data.append(new_line)
                    elif (join_type == "full_outer" or join_type == "outer"):
                        if (rvals1_str not in common_keys.keys()):
                            new_data.append(new_line)
                    else:
                        raise Exception("Unknown join type:", join_type)

        return TSV(new_header, new_data)

    # method to do map join. The right side is stored in a hashmap. only applicable to inner joins
    def natural_join(self, that, inherit_message = ""):
        # check for empty
        if (self.has_empty_header()):
            utils.warn("natural_join: empty tsv")
            return that

        # find the list of common cols
        keys = sorted(list(set(self.get_header_fields()).intersection(set(that.get_header_fields()))))

        # create hashmap
        rmap = {}
        for line in that.get_data():
            fields = line.split("\t")
            rvalues_k = []
            rvalues_v = []

            # generate the key string
            for k in keys:
                rvalues_k.append(fields[that.header_map[k]])

            # generate the value string
            for h in that.get_header_fields():
                if (h not in keys):
                    rvalues_v.append(fields[that.header_map[h]])

            # append to the rmap
            rvalues_key_str = "\t".join(rvalues_k)

            # create the keys if it does not exist
            if (rvalues_key_str not in rmap.keys()):
                rmap[rvalues_key_str] = []

            # append the values
            rmap[rvalues_key_str].append(rvalues_v)

        # create new header
        new_header_fields = []

        # create the keys
        for k in self.get_header_fields():
            new_header_fields.append(k)

        # take only the value part from right side
        for k in that.get_header_fields():
            if (k not in keys):
                new_header_fields.append(k)

        # construct new_header
        new_header = "\t".join(new_header_fields)

        # iterate through left side and add new values using the hash_map
        new_data = []
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("natural_join: [1/1] adding values from hashmap", inherit_message, counter, len(self.data))

            # split line and get fields
            fields = line.split("\t")

            # get the key and value
            lvalues_k = []
            lvalues_v = []

            # generate the keys string
            for k in keys:
                lvalues_k.append(fields[self.header_map[k]])

            # create the value part
            for h in self.get_header_fields():
                if (h not in keys):
                    lvalues_v.append(fields[self.header_map[h]])

            # create the key and value str
            lvalue_key_str = "\t".join(lvalues_k)

            # pick the value from hashmap. TODO
            if (lvalue_key_str not in rmap.keys()):
                utils.trace("key not found in right side tsv: {}: {}: {}. This warning might become exception.".format(keys, lvalues_k, rmap.keys()))
            else:
                # get the list of all values from right side
                vs_list = rmap[lvalue_key_str]
                for vs in vs_list:
                    new_data.append("\t".join(utils.merge_arrays([fields, vs])))

        # return
        return TSV(new_header, new_data)

    # public method handling both random and cols based splitting
    def split_batches(self, num_batches, cols = None, preserve_order = False):
        # check for empty
        if (self.has_empty_header()):
            # check for cols
            if (cols is None):
                utils.warn("split_batches: empty tsv")
                return self
            else:
                raise Exception("split_batches: empty tsv")

        # check if cols are defined or not
        if (cols is None):
            return self.__split_batches_randomly__(num_batches, preserve_order = preserve_order)
        else:
            return self.__split_batches_by_cols__(num_batches, cols)

    # split method to split randomly
    def __split_batches_randomly__(self, num_batches, preserve_order = False):
        # create array to store result
        xtsv_list = []
        data_list = []

        # compute effective number of batches
        effective_batches = int(min(num_batches, self.num_rows()))
        batch_size = int(math.ceil(self.num_rows() / effective_batches))

        # initialize the arrays to store data
        for i in range(effective_batches):
            data_list.append([])

        # iterate to split data
        for i in range(len(self.data)):
            # check if original order of data needs to be preserved
            if (preserve_order == True):
                batch_index = int(i / batch_size)
            else:
                batch_index = i % effective_batches

            # append to the splits data
            data_list[batch_index].append(self.data[i])

        # create list of xtsvs
        for i in range(effective_batches):
            xtsv_list.append(TSV(self.header, data_list[i]))

        # return
        return xtsv_list

    # split method to split tsv into batches
    def __split_batches_by_cols__(self, num_batches, cols):
        # get matching cols
        cols = self.__get_matching_cols__(cols)

        # create some temp column names
        temp_col1 = "__split_batches_by_cols__:hash:{}".format(num_batches)
        temp_col2 = "__split_batches_by_cols__:batch:{}".format(num_batches)

        # generate hash
        hashed_tsv = self.generate_key_hash(cols, temp_col1)

        # take all uniq value of hashes
        hashes = hashed_tsv.col_as_array_uniq(temp_col1)

        # assign some batch number to each hash
        hash_indexes = {}
        for hash_value in hashes:
            hash_indexes[str(hash_value)] = int(hash_value) % num_batches
        hashed_tsv2 = hashed_tsv.transform(temp_col1, lambda t: hash_indexes[t], temp_col2)

        # create new tsvs data
        new_data_list = []
        new_tsvs = []
        for i in range(num_batches):
            new_data_list.append([])

        # assign each record to its correct place
        batch_index = hashed_tsv2.get_col_index(temp_col2)
        for line in hashed_tsv2.get_data():
            fields = line.split("\t")
            batch_id = int(fields[batch_index])
            new_data_list[batch_id].append(line)

        # create each tsv
        for i in range(len(new_data_list)):
            new_tsv = TSV(hashed_tsv2.get_header(), new_data_list[i]) \
                .drop([temp_col1, temp_col2])
            new_tsvs.append(new_tsv)

        # return
        return new_tsvs

    # method to generate a hash for a given set of columns
    def generate_key_hash(self, cols, new_col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("generate_key_hash: empty tsv")

        # resolve cols
        cols = self.__get_matching_cols__(cols)
        indexes = self.__get_col_indexes__(cols)

        # validation
        if (new_col in self.columns()):
            raise Exception("new column already exists: {}".format(new_col))

        # generate deterministic hash
        new_header = "\t".join([self.header, new_col])
        new_data = []

        # iterate
        for line in self.data:
            fields = line.split("\t")
            values = []
            for i in indexes:
                values.append(str(fields[i]))

            # generate a single value
            value_str = "\t".join(values)

            # apply hash
            hash_value = str(utils.compute_hash(value_str))

            # add to new data
            new_data.append("\t".join([line, hash_value]))

        # return
        return TSV(new_header, new_data)

    def cumulative_sum(self, col, new_col, as_int = True):
        # check empty
        if (self.has_empty_header()):
            raise Exception("cumulative_sum: empty tsv")

        # check for presence of col
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        # check for validity of new col
        if (new_col in self.header_map.keys()):
            raise Exception("New column already exists:", str(new_col), str(self.header_fields))

        # create new header
        new_header = self.header + "\t" + new_col

        # create new data
        new_data = []

        # cumulative sum
        cumsum = 0

        col_index = self.header_map[col]

        # iterate
        for line in self.data:
            fields = line.split("\t")
            col_value = float(fields[col_index])
            cumsum += col_value
            if (as_int == True):
                new_line = line + "\t" + str(int(cumsum))
            else:
                new_line = line + "\t" + str(cumsum)

            new_data.append(new_line)

        # return
        return TSV(new_header, new_data)

    def replicate_rows(self, col, new_col = None, max_repl = 0):
        # check empty
        if (self.has_empty_header()):
            raise Exception("replicate_rows: empty tsv")

        # check for presence of col
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        # create new column if it is not existing
        if (new_col is None):
            new_col = "{}:replicate_rows".format(col)

        # check new col
        if (new_col in self.header_map.keys()):
            raise Exception("New Column already exists:", str(new_col), str(self.header_fields))

        # create data
        new_data = []
        new_header = self.header + "\t" + new_col
        for line in self.data:
            fields = line.split("\t")
            col_value = int(fields[self.header_map[col]])
            # check for guard conditions
            if (max_repl > 0 and col_value > max_repl):
                raise Exception("repl_value more than max_repl:", col_value, max_repl)

            # replicate
            for i in range(col_value):
                new_data.append(line + "\t" + "1")

        return TSV(new_header, new_data)

    # TODO: Need better naming. The suffix semantics have been changed.
    def explode(self, cols, exp_func, prefix, default_val = None, collapse = True, ignore_if_missing = False, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("explode: empty tsv", ignore_if_missing)
            return self

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)
        indexes = self.__get_col_indexes__(matching_cols)

        # check for no matching cols
        if (len(matching_cols) == 0):
            utils.raise_exception_or_warn("explode: no matching cols: {}".format(cols), ignore_if_missing)
            return self

        # iterate
        exploded_values = []
        counter = 0
        for line in self.data:
            # report progress
            counter = counter + 1
            utils.report_progress("explode: [1/2] calling explode function", inherit_message, counter, len(self.data))

            # process data
            fields = line.split("\t")
            col_values_map = {}
            for i in indexes:
                col_values_map[self.header_fields[i]] = fields[i]
            exploded_values.append(exp_func(col_values_map))

        # get the list of keys
        exploded_keys = {}
        for exploded_value_list_map in exploded_values:
            for evm in exploded_value_list_map:
                for k, v in evm.items():
                    # validation
                    if (len(k) == 0):
                        raise Exception("Invalid key in the hashmap:{}: {}".format(k, v))
                    exploded_keys[k] = 1

        # create an ordered list of keys
        exploded_keys_sorted = sorted(list(exploded_keys.keys()))

        # new header and data
        new_data = []

        # create header
        new_header_fields = []
        if (collapse == True):
            for j in range(len(self.header_fields)):
                if (j not in indexes):
                    new_header_fields.append(self.header_fields[j])
        else:
            # take care of not referencing self.header_fields
            for h in self.header_fields:
                new_header_fields.append(h)

        # create new names based on suffix
        exploded_keys_new_names = []

        # append new names to the exploded keys
        for e in exploded_keys_sorted:
            exploded_keys_new_names.append(prefix + ":" + e)

        # check if any of new keys clash with old columns
        for k in exploded_keys_new_names:
            if (k in self.get_header_fields()):
                raise Exception("Column already exist:", k, str(self.header_fields))

        # append to the new_header_fields
        for h in exploded_keys_new_names:
            new_header_fields.append(h)
        new_header = "\t".join(new_header_fields)

        # iterate and generate new data
        utils.print_code_todo_warning("explode: Verify this logic is not breaking anything. check TODO")

        counter = 0
        for i in range(len(self.data)):
            # report progress
            counter = counter + 1
            utils.report_progress("explode: [2/2] generating data", inherit_message, counter, len(self.data))

            # process data
            line = self.data[i]
            fields = line.split("\t")

            # get the new list of fields
            new_fields = []
            if (collapse == True):
                for j in range(len(fields)):
                    if (j not in indexes):
                        new_fields.append(fields[j])
            else:
                # take care of not messing up with old fields
                for f in fields:
                    new_fields.append(f)

            # take care of this as the new_fields can be empty. TODO
            #new_line = "\t".join(new_fields) if (len(new_fields) > 0) else ""

            # get the exploded list of maps
            exploded_value_list_map = exploded_values[i]

            for evm in exploded_value_list_map:
                new_vals = []
                for k in exploded_keys_sorted:
                    # add to the output cols
                    if (k in evm.keys()):
                        new_vals.append(str(evm[k]))
                    else:
                        if (default_val is not None):
                            new_vals.append(str(default_val))
                        else:
                            raise Exception("Not all output values are returned from function:", str(evm), str(exploded_keys_sorted))

                # TODO: move this to a proper function
                new_data.append("\t".join(utils.merge_arrays([new_fields, new_vals])))

        # result. json expansion needs to be validated because of potential noise in the data.
        return TSV(new_header, new_data) \
            .validate()

    def __explode_json_transform_func__(self, col, accepted_cols, excluded_cols, single_value_list_cols, transpose_col_groups, merge_list_method, url_encoded_cols,
        nested_cols, collapse_primitive_list, join_col = ","):

        json_explode_index = "__explode_json_index__"

        def __explode_json_transform_func_inner__(mp):
            # some validation.
            if (col not in mp.keys() or mp[col] == "" or mp[col] is None):
                utils.trace("__explode_json_transform_func_inner__: potentially invalid json response found. Usually it is okay. But better to check: {}, {}".format(col, mp))
                mp = {"__explode_json_len__": "0"}
                return [mp]

            # fetch the json string, it might be url encoded or not though recommendation is to use url encoding.
            json_str = mp[col]
            json_mp = None

            # best effort in detecting the type and parsing for robustness
            if (json_str.startswith("%7B") == False and json_str.startswith("%5B") == False):
                if (json_str.startswith("{") or json_str.startswith("[")):
                    utils.warn("explode_json called with column that is not url encoded json. Assuming plain json string")
                    json_mp = json.loads(json_str)
                else:
                    json_str10 = json_str[0:10] + "..." if (len(json_str) > 10) else json_str
                    utils.warn("explode_json called with invalid value in the string. Ignoring parsing: {}".format(json_str10))
                    mp = {"__explode_json_len__": "0"}
                    return [mp]
            else:
                json_mp = json.loads(utils.url_decode(json_str))

            # call internal methods
            results = __explode_json_transform_func_inner_helper__(json_mp)

            # return
            return results

        def __explode_json_transform_func_inner_helper__(json_mp):
            # validation
            if (transpose_col_groups is not None and merge_list_method == "join"):
                raise Exception("transpose_col_groups can not be used with join method. Please use cogroup or unset transpose_col_groups")

            # check if top level is a list
            if (isinstance(json_mp, list)):
                utils.debug("top level is a list. converting to a map")
                return __explode_json_transform_func_inner_helper__({col: json_mp})

            # use inner functions to parse the json
            results = __explode_json_transform_func_expand_json__(json_mp)

            # return
            return results

        # TODO: Do the url encoding outside as a common thing
        def __explode_json_transform_func_expand_json__(json_mp, parent_prefix = None):
            # create some basic structures
            results = []
            single_results = {}
            list_results_arr = []
            dict_results = []

            # iterate over all key values
            for k in json_mp.keys():
                # check for inclusion and exclusion
                if (accepted_cols is not None and k not in accepted_cols):
                    continue
                if (excluded_cols is not None and k in excluded_cols):
                    continue

                # create this combo key for recursion
                parent_with_child_key = parent_prefix + ":" + k if (parent_prefix is not None) else k

                # get value
                v = json_mp[k]

                # handle null scenario. json string can have a special value called null to represent empty or null value, which is converted to None in json parser.
                # such null value should be okay to read as empty string
                if (v is None):
                    utils.trace("__explode_json_transform_func_expand_json__: None type value found. Taking it as empty string. Key: {}".format(k))
                    v = ""

                # handle nested_cols scenario where the entire value is to be set as url encoded json blob
                if (nested_cols is not None and k in nested_cols):
                    single_results[k + ":json:url_encoded"] = utils.url_encode(json.dumps(v))
                # for each data type, there is a different kind of handling
                elif (isinstance(v, (str, int, float))):
                    v1 = utils.strip_spl_white_spaces(v)
                    # check if encoding needs to be done
                    if (url_encoded_cols is not None and k in url_encoded_cols):
                        v1 = utils.url_encode(v1)
                        single_results[k + ":url_encoded"] = v1
                    else:
                        single_results[k] = v1
                else:
                    # TODO :Added on 2021-11-27. Need the counts for arrays and dict to handle 0 count errors. Splitting the single if-elif-else to two level
                    if (isinstance(v, list) and len(v) > 0):
                        single_results[k + ":__explode_json_len__"] = str(len(v))
                        if (len(list_results_arr) > 0 and utils.is_debug()):
                            utils.warn("explode_json: multiple lists are not fully supported. Confirm data parsing or Use accepted_cols or excluded_cols: {}".format(str(k)))

                        # create a new entry for holding the list array
                        list_results_arr.append([])

                        # check for base data types
                        if (isinstance(v[0], (str,int,float))):
                            # treat primitive lists as single value or as yet another list
                            if (collapse_primitive_list == True):
                                # do the encoding
                                v1 = join_col.join(sorted(list([utils.strip_spl_white_spaces(t) for t in v])))
                                if (url_encoded_cols is not None and k in url_encoded_cols):
                                    v1 = utils.url_encode(v1)
                                    single_results[k + ":url_encoded"] = v1
                                else:
                                    single_results[k] = v1
                            else:
                                # iterate through all the values
                                i = 0
                                for vt in v:
                                    # create map to store values
                                    mp2_new = {}

                                    # do the encoding
                                    v1 = utils.strip_spl_white_spaces(vt)
                                    if (url_encoded_cols is not None and k in url_encoded_cols):
                                        v1 = utils.url_encode(v1)
                                        mp2_new[k + ":url_encoded"] = v1
                                    else:
                                        mp2_new[k] = v1

                                    # assign index
                                    mp2_new[json_explode_index] = str(i)
                                    i = i + 1

                                    # append the map to the list
                                    list_results_arr[-1].append(mp2_new)
                        elif (isinstance(v[0], dict)):
                            # append all the expanded version of the list
                            for v1 in v:
                                 mp2_list = __explode_json_transform_func_expand_json__(v1, parent_prefix = parent_with_child_key)
                                 for mp2 in mp2_list:
                                     mp2_new = {}
                                     for k1 in mp2.keys():
                                         mp2_new[k + ":" + k1] = mp2[k1]
                                     list_results_arr[-1].append(mp2_new)
                        elif (isinstance(v[0], list)):
                            # check for non supported case
                            if (len(v) > 1):
                                raise Exception("Inner lists are not supported. Use accepted_cols or excluded_cols: {}, number of values:{}".format(str(k)), len(v))

                            # check if there is flag to use only the first column
                            if (single_value_list_cols is not None and k in single_value_list_cols):
                                 mp2_list = __explode_json_transform_func_expand_json__(v[0], parent_prefix = parent_with_child_key)
                                 i = 0
                                 for mp2 in mp2_list:
                                     mp2_new = {}
                                     for k1 in mp2.keys():
                                         mp2_new[k + ":" + k1] = mp2[k1]
                                     mp2_new[json_explode_index] = str(i)
                                     i = i + 1
                                     list_results_arr[-1].append(mp2_new)
                            else:
                                raise Exception("Inner lists are not supported. Use accepted_cols or excluded_cols: {}".format(str(k)))
                        else:
                            raise Exception("Unknown data type:", type(v[0]))
                    elif (isinstance(v, dict) and len(v) > 0):
                        # warn for non trivial case
                        if (len(dict_results) > 0 and utils.is_debug()):
                            utils.warn("explode_json: multiple maps are not fully supported. Confirm data parsing or Use accepted_cols or excluded_cols: {}".format(str(k)))

                        # recursive call
                        mp2_list = __explode_json_transform_func_expand_json__(v, parent_prefix = parent_with_child_key)

                        # check if it was a flat hashmap or a nested. if flat, use dict_list else use list_results_arr
                        if (len(mp2_list) > 1):
                            list_results_arr.append([])

                            # create a new map with correct key
                            i = 0
                            for mp2 in mp2_list:
                                mp2_new = {}
                                for k1 in mp2.keys():
                                    mp2_new[k + ":" + k1] = mp2[k1]
                                mp2_new[json_explode_index] = str(i)
                                i = i + 1
                                list_results_arr[-1].append(mp2_new)
                        else:
                           # create a new map with correct key
                           i = 0
                           for mp2 in mp2_list:
                               mp2_new = {}
                               for k1 in mp2.keys():
                                   mp2_new[k + ":" + k1] = mp2[k1]
                               mp2_new[json_explode_index] = str(i)
                               i = i + 1
                               dict_results.append(mp2_new)
                    elif (len(v) == 0):
                        pass
                    else:
                        raise Exception("Unknown data type: {}, {}".format(k, type(v)))

            # now collapse single_results and dict_results into one giant hashmap
            combined_map = {}
            for k in single_results.keys():
                combined_map[k] = single_results[k]
            for mp in dict_results:
                for k in mp.keys():
                    combined_map[k] = str(mp[k])

            # look for prefixes that needed to be transposed and moved from combined map to list_results_arr
            if (transpose_col_groups is not None):
                utils.warn("This transpose_col_groups parameter is experimental at this point and may not work fully")

                # iterate over all transpose groups
                for transpose_col_group_prefix in transpose_col_groups:
                    # TODO: the prefix should not end with ":"
                    if (transpose_col_group_prefix.endswith(":")):
                        raise Exception("WIP API. Dont use prefix name with ':' though the colon will be used as separator.", transpose_col_group_prefix)

                    # variables to store results
                    key_list_results = []
                    value_list_results = []
                    keys_found = {}

                    # iterate and find all the matching ones
                    for k in combined_map.keys():
                        if (parent_prefix is not None and parent_prefix == transpose_col_group_prefix):
                            key_list_results.append({ "__key__": str(k) })
                            value_list_results.append({ "__value__": str(combined_map[k]) })
                            keys_found[k] = 1

                    # remove all the keys that were found
                    for k in keys_found.keys():
                        del combined_map[k]

                    # append to the list_results_arr
                    list_results_arr.append(key_list_results)
                    list_results_arr.append(value_list_results)

            # create a common variable for merged list
            combined_merge_list = []

            # check for full join or cogroup
            if (merge_list_method == "cogroup"):
                # do join or cogroup for the list_results_arr
                cogroup_max = 0
                for list_results in list_results_arr:
                    cogroup_max = len(list_results) if (len(list_results) > cogroup_max) else cogroup_max
                for list_results in list_results_arr:
                    list_results_len = len(list_results)
                    for i in range(cogroup_max - list_results_len):
                        list_results.append({})

                # do a cogroup
                cogroup_list = []
                for i in range(cogroup_max):
                    cogroup_list.append({})
                    for list_results in list_results_arr:
                        mp = list_results[i]
                        for k in mp.keys():
                            cogroup_list[-1][k] = str(mp[k])

                # assign to combined_merge_list
                combined_merge_list = cogroup_list
            elif (merge_list_method == "join"):
                # call a function to do the combinatorial join
                combined_merge_list = __explode_json_transform_func_join_lists__(list_results_arr)
            else:
                raise Exception("Unknown merge_list_method:", merge_list_method)

            # merge combined map with cogroup
            if (len(combined_merge_list) > 0):
                 for mp in combined_merge_list:
                     mp_new = {}
                     for k in mp.keys():
                         mp_new[k] = str(mp[k])
                     for k in combined_map.keys():
                         mp_new[k] = str(combined_map[k])
                     results.append(mp_new)
            else:
                results.append(combined_map)

            # return
            return results

        def __explode_json_transform_func_join_lists__(list_results_arr):
            if (len(list_results_arr) == 0):
                return {}

            if (len(list_results_arr) == 1):
                return list_results_arr[0]

            results = []
            joined_list = __explode_json_transform_func_join_lists__(list_results_arr[1:])
            for i in range(len(joined_list)):
                mp_new = {}
                for mp_i in list_results_arr[0]:
                    for mp_j in joined_list:
                        mp_new = {}
                        for k in mp_i.keys():
                            mp_new[k] = str(mp_i[k])
                        for k in mp_j.keys():
                            mp_new[k] = str(mp_j[k])
                        results.append(mp_new)

            return results

        # return the inner function
        return __explode_json_transform_func_inner__

    # TODO: Need better name
    # TODO: the json col is expected to be in url_encoded form otherwise does best effort guess
    # TODO: url_encoded_cols, excluded_cols, accepted_cols are actually json hashmap keys and not xpath
    # TODO: __explode_json_index__ needs to be tested and confirmed
    # TODO: need proper xpath based exclusion to better handle noise
    def explode_json(self, col, prefix = None, accepted_cols = None, excluded_cols = None, single_value_list_cols = None, transpose_col_groups = None,
        merge_list_method = "cogroup", collapse_primitive_list = True, url_encoded_cols = None, nested_cols = None, collapse = True, ignore_if_missing = False, inherit_message = ""):

        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("explode_json: empty tsv", ignore_if_missing)
            return self

        # warn
        if (excluded_cols is not None):
            utils.print_code_todo_warning("explode_json: excluded_cols is work in progress and may not work in all scenarios")

        # validation
        if (col not in self.header_map.keys()):
            utils.raise_exception_or_warn("Column not found: {}, {}".format(str(col), str(self.header_fields)), ignore_if_missing)
            return self

        # warn on risky combinations
        if (merge_list_method == "cogroup"):
            utils.print_code_todo_warning("explode_json: merge_list_method = cogroup is only meant for data exploration. Use merge_list_method = join for generating all combinations for multiple list values")

        # name prefix
        if (prefix is None):
            utils.warn("explode_json: prefix is None. Using col as the name prefix")
            prefix = col

        # check for explode function
        exp_func = self.__explode_json_transform_func__(col, accepted_cols = accepted_cols, excluded_cols = excluded_cols, single_value_list_cols = single_value_list_cols,
            transpose_col_groups = transpose_col_groups, merge_list_method = merge_list_method, url_encoded_cols = url_encoded_cols, nested_cols = nested_cols, collapse_primitive_list = collapse_primitive_list)

        # use explode to do this parsing
        inherit_message2 = inherit_message + ": explode_json" if (inherit_message != "") else "explode_json"
        return self \
            .add_seq_num(prefix + ":__json_index__", inherit_message = "explode_json") \
            .explode([col], exp_func, prefix = prefix, default_val = "", collapse = collapse, inherit_message = inherit_message2) \
            .validate()

    def transpose(self, n = 1):
        return self.take(n).__transpose_topn__()

    def __transpose_topn__(self):
        # construct new header
        new_header_fields = []
        new_header_fields.append("col_name")
        for i in range(self.num_rows()):
            new_header_fields.append("row:" + str(i + 1))
        new_header = "\t".join(new_header_fields)

        # create col arrays and new_data
        new_data = []
        for h in self.header_fields:
            new_fields = []
            new_fields.append(h)
            for v in self.col_as_array(h):
                new_fields.append(v)
            new_data.append("\t".join(new_fields))

        # return
        return TSV(new_header, new_data)

    # this method converts the rows into columns. very inefficient
    def reverse_transpose(self, grouping_cols, transpose_key, transpose_cols, default_val = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("reverse_transpose empty tsv")

        utils.print_code_todo_warning("reverse_transpose: is not implemented efficiently")
        # resolve the grouping and transpose_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)
        transpose_cols = self.__get_matching_cols__(transpose_cols)

        # get the columns to be selected for each tsv
        sel_cols = []
        for c in grouping_cols:
            sel_cols.append(c)
        for c in transpose_cols:
            sel_cols.append(c)

        # get the list of uniq values and corresponding tsvs
        uniq_vals = sorted(self.col_as_array_uniq(transpose_key))
        result = self.select(grouping_cols).distinct()

        # iterate over tsv of each unique value
        for i in range(len(uniq_vals)):
            v = uniq_vals[i]
            result = result.join(self.eq_str(transpose_key, v).select(sel_cols), grouping_cols, rsuffix = "{}:{}".format(transpose_key, v), join_type = "left", default_val = default_val)

        # return result
        return result

    def flatmap(self, col, func, new_col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("flatmap: empty tsv")

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found:", str(col), str(self.header_fields))

        # check for new column
        if (new_col in self.header_map.keys()):
            raise Exception("New Column already exists:", str(new_col), str(self.header_fields))

        # create new data
        new_data = []
        new_header = self.header + "\t" + new_col

        # iterate
        for line in self.data:
            fields = line.split("\t")
            col_value = fields[self.header_map[col]]
            new_vals = func(col_value)
            for new_val in new_vals:
                new_data.append(line + "\t" + new_val)

        return TSV(new_header, new_data)

    def to_tuples(self, cols, inherit_message = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("to_tuples: empty tsv")

        # validate cols
        for col in cols:
            if (self.has_col(col) == False):
                raise Exception("col doesnt exist:", col, str(self.header_fields))

        # select the cols
        result = []

        # progress counters
        counter = 0
        inherit_message2 = inherit_message + ": to_tuples" if (inherit_message != "") else inherit_message
        for line in self.select(cols, inherit_message = inherit_message2).get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("to_tuples: [1/1] converting to tuples", inherit_message, counter, len(self.data))

            fields = line.split("\t")
            result.append(self.__expand_to_tuple__(fields))

        # return
        return result

    def __expand_to_tuple__(self, vs):
        # apply a switch case to convert to the tuples
        if (len(vs) == 1):
            return (vs[0])
        elif (len(vs) == 2):
            return (vs[0], vs[1])
        elif (len(vs) == 3):
            return (vs[0], vs[1], vs[2])
        elif (len(vs) == 4):
            return (vs[0], vs[1], vs[2], vs[3])
        elif (len(vs) == 5):
            return (vs[0], vs[1], vs[2], vs[3], vs[4])
        elif (len(vs) == 6):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5])
        elif (len(vs) == 7):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6])
        elif (len(vs) == 8):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7])
        elif (len(vs) == 9):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7], vs[8])
        elif (len(vs) == 10):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7], vs[8], vs[9])
        elif (len(vs) == 11):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7], vs[8], vs[9], vs[10])
        elif (len(vs) == 12):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7], vs[8], vs[9], vs[10], vs[11])
        elif (len(vs) == 13):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7], vs[8], vs[9], vs[10], vs[11], vs[12])
        elif (len(vs) == 14):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7], vs[8], vs[9], vs[10], vs[11], vs[12], vs[13])
        elif (len(vs) == 15):
            return (vs[0], vs[1], vs[2], vs[3], vs[4], vs[5], vs[6], vs[7], vs[8], vs[9], vs[10], vs[11], vs[12], vs[13], vs[14])
        else:
            raise Exception("Length of values is more than 10. Not supported." + str(vs))

    # this method sets the missing values for columns
    def set_missing_values(self, cols, default_val, ignore_if_missing = False, inherit_message = ""):
        inherit_message2 = inherit_message + ": set_missing_values" if (len(inherit_message) > 0) else "set_missing_values"
        return self.transform_inline(cols, lambda x: x if (x != "") else default_val, ignore_if_missing = ignore_if_missing, inherit_message = inherit_message2)

    # calls class that inherits TSV
    def extend_class(self, newclass, *args, **kwargs):
        return newclass(self.header, self.data, *args, **kwargs)

    # calls class that inherits TSV. TODO: forgot the purpose
    def extend_external_class(self, newclass, *args, **kwargs):
        utils.warn("extend_external_class: not sure the purpose of this method now. use extend_class instead")
        return newclass(self.header, self.data, *args, **kwargs)

    # custom function to call user defined apis
    def custom_func(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    # taking the clipboard functionality from pandas
    def to_clipboard(self):
        self.to_df().to_clipboard()
        return self

    def __convert_to_maps__(self):
        result = []
        for line in self.data:
            mp = {}
            fields = line.split("\t")
            for h in self.header_map.keys():
                mp[h] = fields[self.header_map[h]]

            result.append(mp)

        return result

    # placeholder for new method to do xpath based filtering for json blobs
    def filter_json_by_xpath(self, col, xpath_filter, inherit_message = ""):
        raise Exception("Not implemented yet")

    def get_col_index(self, col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("get_col_index: empty tsv")

        # validation
        if (col not in self.columns()):
            raise Exception("Column not found: {}".format(col))

        # return
        return self.header_map[col]

    # method to return a unique hash for the tsv objet
    def get_hash(self):
        # check empty
        if (self.has_empty_header()):
            utils.warn("get_hash: empty tsv")

        # create array
        hashes = []

        # hash of header
        hashes.append("{}".format(utils.compute_hash(self.header)))

        # hash of data
        for line in self.data:
            hashes.append("{}".format(utils.compute_hash(line)))

        # return as string
        return "{}".format(utils.compute_hash(",".join(hashes)))

    # TODO: confusing name. is_empty can also imply no data
    def is_empty(self):
        return self.get_header() == ""

    def has_empty_header(self):
        return self.get_header() == ""

    def __has_all_int_values__(self, col):
        # check for empty
        if (self.num_rows() == 0):
            return False

        # TODO: is there a better way to call is numeric
        try:
            for v in self.col_as_array(col):
                if (utils.isnumeric(v) == False):
                    return False
        except:
            return False

        # return
        return True

    def __has_all_float_values__(self, col):
        # check for empty
        if (self.num_rows() == 0):
            return False

        # TODO: is there a better way to call is numeric
        try:
            for v in self.col_as_array(col):
                if (utils.is_float(v) == False):
                    return False
        except:
            return False

        # return
        return True

    # this is a utility function that takes list of column names that support regular expression.
    # col_or_cols is a special variable that can be either single column name or an array. python
    # treats a string as an array of characters, so little hacky but a more intuitive api wise
    def __get_matching_cols__(self, col_or_cols, ignore_if_missing = False):
        # handle boundary conditions
        if (col_or_cols is None or len(col_or_cols) == 0):
            return []

        # check if there is comma. If yes, then map it to array
        if ("," in col_or_cols):
            col_or_cols = col_or_cols.split(",")

        # check if this is a single col name or an array
        is_array = utils.is_array_of_string_values(col_or_cols)

        # create name
        col_patterns = []
        if (is_array == True):
            col_patterns = col_or_cols
        else:
            col_patterns.append(col_or_cols)

        # now iterate through all the column names, check if it is a regular expression and find
        # all matching ones
        matching_cols = []
        for col_pattern in col_patterns:
            # check for matching columns for the pattern
            col_pattern_found = False

            # iterate through header
            for h in self.header_fields:
                # check for match
                if ((col_pattern.find(".*") != -1 and re.match(col_pattern, h) is not None) or (col_pattern == h)):
                    col_pattern_found = True
                    # if not in existing list then add
                    if (h not in matching_cols):
                        matching_cols.append(h)

            # raise exception if some col or pattern is not found
            if (col_pattern_found == False):
                utils.raise_exception_or_warn("Col name or pattern not found: {}, {}".format(col_pattern, str(self.header_fields)), ignore_if_missing)
                return []

        # return
        return matching_cols

    def __has_matching_cols__(self, col_or_cols, ignore_if_missing = False):
        try:
            if (len(self.__get_matching_cols__(col_or_cols, ignore_if_missing = ignore_if_missing)) > 0):
                return True
            else:
                return False
        except:
            return False

    def __get_col_indexes__(self, cols):
        indexes = []
        for c in cols:
            indexes.append(self.header_map[c])

        # return
        return indexes

    # this method prints message so that the transformation have some way of notifying what is going on
    def print(self, msg):
        print(msg)
        return self

    # print some status
    def print_stats(self, msg):
        msg2 = "{}: num_rows: {}, num_cols: {}".format(msg, self.num_rows(), self.num_cols())
        print(msg2)
        return self

def get_version():
    return "v0.3.9:empty_tsv"

def get_func_name(f):
    return f.__name__

def get_rolling_func_init(func_name):
    if (func_name == "sum"):
        return [0]
    elif (func_name == "min"):
        return [float('inf')]
    elif (func_name == "max"):
        return [float('-inf')]
    elif (func_name == "mean"):
        return [0, 0]
    elif (func_name == "len"):
        return [0]
    else:
        raise Exception("rolling agg func not supported:", func_name)

def get_rolling_func_update(arr, v, func_name):
    if (func_name == "sum"):
        arr[0] = arr[0] + v
    elif (func_name == "min"):
        arr[0] = min(arr[0], v)
    elif (func_name == "max"):
        arr[0] = max(arr[0], v)
    elif (func_name == "mean"):
        arr[0] = arr[0] + v
        arr[1] = arr[1] + 1
    elif (func_name == "len"):
        arr[0] = arr[0] + 1
    else:
        raise Exception("rolling agg func not supported:", func_name)

def get_rolling_func_update_sum(arr, v):
    arr[0] = arr[0] + v

def get_rolling_func_update_mean(arr, v):
    arr[0] = arr[0] + v
    arr[1] = arr[1] + 1

def get_rolling_func_update_min(arr, v):
    arr[0] = min(arr[0], v)

def get_rolling_func_update_max(arr, v):
    arr[0] = max(arr[0], v)

def get_rolling_func_update_len(arr, v):
    arr[0] = arr[0] + 1

def get_rolling_func_closing(arr, func_name):
    if (func_name == "sum"):
        return arr[0]
    elif (func_name == "min"):
        return arr[0]
    elif (func_name == "max"):
        return arr[0]
    elif (func_name == "mean"):
        if (arr[1] > 0):
            return arr[0] / arr[1]
        else:
            print("Divide by zero. Returning 0")
            return 0
    elif (func_name == "len"):
        return arr[0]
    else:
        raise Exception("rolling agg func not supported:", func_name)

def read(paths, sep = None):
    return tsvutils.read(paths, sep)

def write(xtsv, path):
    return tsvutils.save_to_file(xtsv, path)

def merge(xtsvs, def_val_map = None):
    # warn if def_val_map is not defined
    if (def_val_map is None):
        utils.warn("merge: use merge_union or merge_intersect")

    # return
    return tsvutils.merge(xtsvs, def_val_map = def_val_map)

def merge_union(xtsvs, def_val_map = {}):
    # check def_val_map
    if (def_val_map is None):
        raise Exception("merge_union: def_val_map can not be none for union. Use merge_intersect instead")

    # return
    return tsvutils.merge(xtsvs, def_val_map = def_val_map)

def merge_intersect(xtsvs):
    return tsvutils.merge(xtsvs, def_val_map = None)

def exists(path):
    return tsvutils.check_exists(path)

def from_df(df):
    return tsvutils.from_df(df)

def from_maps(mps):
    return tsvutils.load_from_array_of_map(mps)

def enable_debug_mode():
    utils.enable_debug_mode()

def disable_debug_mode():
    utils.disable_debug_mode()

def set_report_progress_perc(perc):
    utils.set_report_progress_perc(perc)

def set_report_progress_min_thresh(thresh):
    utils.set_report_progress_min_thresh(thresh)

# factory method
def newWithCols(cols, data = []):
    utils.warn("newWithCols is deprecated. Use new_with_cols instead")
    return new_with_cols(cols, data = data)

def new_with_cols(cols, data = []):
    return TSV("\t".join(cols), data)

def create_empty():
    return new_with_cols([])

