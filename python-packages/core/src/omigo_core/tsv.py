"""TSV Class"""
import re
import math
import pandas as pd
import random
import json
from omigo_core import utils, tsvutils, funclib
import sys
import time

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
        self.header_fields = list(filter(lambda t: t != "", self.header.split("\t"))) if (self.header != "") else []
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
                raise Exception("Duplicate header key:{}: {}".format(h, str(self.header_fields)))

            self.header_map[h] = i
            self.header_index_map[i] = h

        # basic validation
        if (len(data) > 0 and len(data[0].split("\t")) != len(self.header_fields)):
            raise Exception("Header length is not matching with data length: len(self.get_header_fields()): {}, len(data[0].fields): {}, header_fields: {}, data[0].fields: {}".format(
                len(self.header_fields), len(data[0].split("\t")), str(self.header_fields), str(data[0].split("\t"))))

    # debugging
    def to_string(self):
        return "Header: {}, Data: {}".format(str(self.header_map), str(len(self.get_data())))

    # check data format
    def validate(self):
        # data validation
        counter = 0
        for line in self.get_data():
            counter = counter + 1
            fields = line.split("\t")
            if (len(fields) != len(self.get_header_fields())):
                raise Exception("Header length is not matching with data length. position: {}, len(header): {}, header: {}, len(fields): {}, fields: {}".format(
                    counter, len(self.get_header_fields()), self.header_fields, len(fields), str(fields)))

        # return
        return self

    def has_col(self, col):
        # validate xcol
        return col in self.header_map.keys()

    # cols is array of string
    def select(self, col_or_cols, dmsg = ""):
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
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("select: [1/1] selecting columns", dmsg, counter, self.num_rows())

            # get fields
            fields = line.split("\t")
            new_fields = []

            # validation
            for i in indexes:
                if (i >= len(fields)):
                    raise Exception("Invalid index: col_or_cols: {}, matching_cols: {}, indexes: {}, line: {}, fields: {}, len(fields): {}, len(self.get_header_fields()): {}, self.get_header_map(): {}".format(
                        col_or_cols, matching_cols, indexes, line, fields, len(fields), len(self.get_header_fields()), self.header_map))

                # append to new_fields
                new_fields.append(fields[i])

            # append to new data
            new_data.append("\t".join(new_fields))

        # return
        return TSV(new_header, new_data)

    def values_not_in(self, col, values, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "values_not_in")
        return self.filter([col], lambda x: x not in values, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def values_in(self, col, values, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "values_in")
        return self.filter([col], lambda x: x in values, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_match(self, col, pattern, ignore_if_missing = False, dmsg = ""):
        utils.warn("Please use not_regex_match instead")
        return self.not_regex_match(col, pattern, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_regex_match(self, col, pattern, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "not_regex_match")
        return self.regex_match(col, pattern, condition = False, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def match(self, col, pattern, ignore_if_missing = False, dmsg = ""):
        utils.warn("Please use regex_match instead")
        return self.regex_match(col, pattern, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def regex_match(self, col, pattern, condition = True, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "regex_match")
        return self.filter([col], lambda x: (re.match(pattern, x) is not None) == condition, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_eq(self, col, value, ignore_if_missing = False, dmsg = ""):
        utils.warn("This api can have side effects because of implicit data types conversion in python. Use not_eq_int, not_eq_str or not_eq_float")
        dmsg = utils.extend_inherit_message(dmsg, "not_eq")
        return self.filter([col], lambda x: x != value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def eq(self, col, value, ignore_if_missing = False, dmsg = ""):
        utils.warn("This api can have side effects because of implicit data types conversion in python. Use eq_int, eq_str or eq_float")
        return self.filter([col], lambda x: x == value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def eq_int(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "eq_int")
        return self.filter([col], lambda x: int(float(x)) == value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def eq_float(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "eq_float")
        return self.filter([col], lambda x: float(x) == value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def eq_str(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "eq_str")
        return self.filter([col], lambda x: str(x) == str(value), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_eq_int(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "not_eq_int")
        return self.filter([col], lambda x: int(x) != value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_eq_float(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "not_eq_float")
        return self.filter([col], lambda x: float(x) != value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_eq_str(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "not_eq_str")
        return self.filter([col], lambda x: str(x) != str(value), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def is_nonzero(self, col, ignore_if_missing = False, dmsg = ""):
        utils.warn("Deprecated. Use is_nonzero_float() instead")
        dmsg = utils.extend_inherit_message(dmsg, "is_nonzero")
        return self.is_nonzero_float(col, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def is_nonzero_int(self, col, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "is_nonzero_int")
        return self.filter([col], lambda x: int(x) != 0, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def is_nonzero_float(self, col, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "is_nonzero_float")
        return self.filter([col], lambda x: float(x) != 0, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def lt_str(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "lt_str")
        return self.filter([col], lambda x: x < value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def le_str(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "le_str")
        return self.filter([col], lambda x: x <= value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def gt_str(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "gt_str")
        return self.filter([col], lambda x: x > value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def ge_str(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ge_str")
        return self.filter([col], lambda x: x >= value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def gt(self, col, value, ignore_if_missing = False, dmsg = ""):
        utils.warn("Deprecated. Use gt_float() instead")
        dmsg = utils.extend_inherit_message(dmsg, "gt")
        return self.gt_float(col, value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def gt_int(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "gt_int")
        return self.filter([col], lambda x: int(float(x)) > int(float(value)), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def gt_float(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "gt_float")
        return self.filter([col], lambda x: float(x) > float(value), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def ge(self, col, value, ignore_if_missing = False, dmsg = ""):
        utils.warn("Deprecated. Use ge_float() instead")
        dmsg = utils.extend_inherit_message(dmsg, "ge")
        return self.ge_float(col, value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def ge_int(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ge_int")
        return self.filter([col], lambda x: int(float(x)) >= int(float(value)), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def ge_float(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ge_float")
        return self.filter([col], lambda x: float(x) >= float(value), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def lt(self, col, value, ignore_if_missing = False, dmsg = ""):
        utils.warn("Deprecated. Use lt_float() instead")
        dmsg = utils.extend_inherit_message(dmsg, "lt")
        return self.lt_float(col, value, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def lt_int(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "lt_int")
        return self.filter([col], lambda x: int(float(x)) < int(float(value)), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def lt_float(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "lt_float")
        return self.filter([col], lambda x: float(x) < float(value), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def le(self, col, value, ignore_if_missing = False, dmsg = ""):
        utils.warn("Deprecated. Use le_float() instead")
        dmsg = utils.extend_inherit_message(dmsg, "le")
        return self.filter([col], lambda x: float(x) <= float(value), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def le_int(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "le_int")
        return self.filter([col], lambda x: int(float(x)) <= int(float(value)), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def le_float(self, col, value, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "le_float")
        return self.filter([col], lambda x: float(x) <= float(value), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def startswith(self, col, prefix, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "startswith")
        return self.filter([col], lambda x: str(x).startswith(prefix), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_startswith(self, col, prefix, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "not_startswith")
        return self.exclude_filter([col], lambda x: str(x).startswith(prefix), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def endswith(self, col, suffix, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "endswith")
        return self.filter([col], lambda x: str(x).endswith(suffix), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def not_endswith(self, col, suffix, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "not_endswith")
        return self.exclude_filter([col], lambda x: str(x).endswith(suffix), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def is_empty_str(self, col, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "is_empty_str")
        return self.eq_str(col, "", dmsg = dmsg)

    def is_nonempty_str(self, col, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "is_nonempty_str")
        return self.not_eq_str(col, "", dmsg = dmsg)

    def replace_str_inline(self, cols, old_str, new_str, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "replace_str_inline")
        return self.transform_inline(cols, lambda x: x.replace(old_str, new_str), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def group_count(self, cols, prefix = "group", collapse = True, precision = 6, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "group_count")

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
        return self \
            .aggregate(cols, [cols[0]], [funclib.get_len], collapse = collapse, dmsg = dmsg) \
            .rename(cols[0] + ":get_len", new_count_col, dmsg = dmsg) \
            .transform([new_count_col], lambda x: str(int(x) / len(self.get_data())), new_ratio_col, dmsg = dmsg) \
            .reverse_sort(new_count_col, dmsg = dmsg) \
            .apply_precision(new_ratio_col, precision, dmsg = dmsg)

    def ratio(self, col1, col2, new_col, default = 0.0, precision = 6, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ratio")
        return self \
            .transform([col1, col2], lambda x, y: float(x) / float(y) if (float(y) != 0) else default, new_col, dmsg = dmsg) \
            .apply_precision(new_col, precision, dmsg = dmsg)

    def ratio_const(self, col, denominator, new_col, precision = 6, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "ratio_const")
        return self \
            .transform([col], lambda x: float(x) / float(denominator) if (float(denominator) != 0) else default, new_col, dmsg = dmsg) \
            .apply_precision(new_col, precision, dmsg = dmsg)

    def apply_precision(self, cols, precision, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "apply_precision")
        return self.transform_inline(cols, lambda x: ("{:." + str(precision) + "f}").format(float(x)), dmsg = dmsg)

    # TODO: use skip_rows for better name
    def skip(self, count):
        utils.warn_once("use skip_rows instead coz of better name")
        return self.skip(count)

    def skip_rows(self, count):
        return TSV(self.header, self.data[count:])

    def last(self, count):
        # check boundary conditions
        if (count > len(self.get_data())):
            count = len(self.get_data())

        # return
        return TSV(self.header, self.data[-count:])

    def take(self, count, dmsg = ""):
        # return result
        if (count > len(self.get_data())):
            count = len(self.get_data())

        return TSV(self.header, self.data[0:count])

    def distinct(self, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "distinct")

        # create variables
        new_data = []
        key_map = {}

        # iterate
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[1/1] calling function", dmsg, counter, self.num_rows())

            # check if the line doesnt exist already
            if (line not in key_map.keys()):
                key_map[line] = 1
                new_data.append(line)

        # return
        return TSV(self.header, new_data)

    def distinct_cols(self, col_or_cols, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "distinct_cols")
        return self \
            .select(col_or_cols, dmsg = dmsg) \
            .distinct(dmsg = dmsg)
       
    # TODO: use drop_cols instead coz of better name
    def drop(self, col_or_cols, ignore_if_missing = False, dmsg = ""):
        utils.warn_once("use drop_cols instead coz of better name")
        return self.drop_cols(col_or_cols, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def drop_cols(self, col_or_cols, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "drop_cols")

        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("drop: empty tsv", ignore_if_missing)
            return self

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(col_or_cols, ignore_if_missing = ignore_if_missing)

        # find the columns that dont match
        non_matching_cols = []
        for h in self.get_header_fields():
            if (h not in matching_cols):
                non_matching_cols.append(h)

        # return
        return self \
            .select(non_matching_cols, dmsg = dmsg)

    def drop_cols_with_prefix(self, prefix, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "drop_cols_with_prefix")
        return self \
            .drop_cols("{}:.*".format(prefix), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def drop_cols_with_suffix(self, suffix, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "drop_cols_with_suffix")
        return self \
            .drop_cols(".*:{}".format(suffix), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def drop_if_exists(self, col_or_cols, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "drop_if_exists")
        return self \
            .drop_cols(col_or_cols, ignore_if_missing = True, dmsg = dmsg)

    def drop_cols_if_exists(self, col_or_cols, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "drop_cols_if_exists")
        return self.drop_cols(col_or_cols, ignore_if_missing = True, dmsg = dmsg)

    # TODO: the select_cols is not implemented properly
    def window_aggregate(self, win_col, agg_cols, agg_funcs, winsize, select_cols = None, sliding = False, collapse = True, suffix = "", precision = 2, dmsg = ""):
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
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("window_aggregate: [1/1] calling function", dmsg, counter, self.num_rows())

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
                .drop_cols(win_col) \
                .rename(new_win_col, win_col) \
                .aggregate(cols2, agg_cols, agg_funcs, collapse)
        else:
            cols2 = select_cols
            cols2.append(new_win_col)
            return TSV(new_header, new_data) \
                .aggregate(cols2, agg_cols, agg_funcs, collapse, precision)

    # The signature for agg_func is func(list_of_maps). Each map will get the agg_cols
    def group_by_key(self, grouping_cols, agg_cols, agg_func, suffix = "", collapse = True, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("group_by_key: empty tsv")

        # resolve grouping and agg_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)
        agg_cols = self.__get_matching_cols__(agg_cols)

        # check for validity
        if (len(grouping_cols) == 0):
            raise Exception("No input columns: {}".format(grouping_cols))

        # validate grouping cols
        for c in grouping_cols:
            if (c not in self.header_map.keys()):
                raise Exception("grouping col not found: {}, columns: {}".format(c, self.header_fields))

        # validate agg cols
        for c in agg_cols:
            if (c not in self.header_map.keys()):
                raise Exception("agg col not found: {}, columns: {}".format(c, self.header_fields))

        # group all the values in the key
        grouped = {}
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("group_by_key: [1/3] grouping: progress", dmsg, counter, self.num_rows())

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
            utils.report_progress("group_by_key: [2/3] grouping func: progress", dmsg, counter, len(grouped))

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
            # handling empty data
            dummy_response = agg_func([])
            for k in dummy_response.keys():
                agg_out_keys[k] = 1

        # validate that none of the agg_func output keys are conflicting with the original tsvs. TODO
        utils.print_code_todo_warning("Removing this condition for checking of duplicate names. They are already given a suffix so there is no clash.")
        for k in agg_out_keys.keys():
            if (k in self.header_map.keys()):
                utils.print_code_todo_warning("TODO: Old check: Agg func can not output keys that have the same name as original columns: {}, {}".format(k, str(self.get_header_fields())))

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
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("group_by_key: [3/3] generating data", dmsg, counter, self.num_rows())

            # process data
            fields = line.split("\t")

            # create grouping key
            keys = []
            for g in grouping_cols:
                keys.append(fields[self.header_map[g]])

            keys_str = "\t".join(keys)

            # check output data
            if (len(grouped_agg[keys_str]) != len(new_cols)):
                raise Exception("Error in data and number of output cols: {}, {}".format(grouped_agg[keys_str], new_cols))

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
    def arg_min(self, grouping_cols, argcols, valcols, suffix = "arg_min", use_string_datatype = False, topk = 1, sep = "|", collapse = True, dmsg = ""):
        utils.warn_once("arg_min is not implemented correctly. Too complicated")
        dmsg = utils.extend_inherit_message(dmsg, "arg_min")

        # some unsupported case
        if (use_string_datatype == True):
            raise Exception("arg_min: use_string_datatype = True is not supported")

        return self.__arg_min_or_max_common__(grouping_cols, argcols, valcols, suffix, topk, sep, -1, collapse = collapse, dmsg = dmsg)

    def arg_max(self, grouping_cols, argcols, valcols, suffix = "arg_max", use_string_datatype = False, topk = 1, sep = "|", collapse = True, dmsg = ""):
        utils.warn_once("arg_max is not implemented correctly. Too complicated")
        dmsg = utils.extend_inherit_message(dmsg, "arg_max")
        return self.__arg_min_or_max_common__(grouping_cols, argcols, valcols, suffix, use_string_datatype, topk, sep, 1, collapse = collapse, dmsg = dmsg)

    # grouping_cols are for grouping
    # argcols which are returned where valcols values are max or min
    # suffix is added to both arg and val. arg are suffixed as :arg, values are suffixed as val1, val2 upto topk
    def __arg_min_or_max_common__(self, grouping_cols, argcols, valcols, suffix, use_string_datatype, topk, sep, sign, collapse = False, dmsg = ""):
        grouping_cols = self.__get_matching_cols__(grouping_cols)
        argcols = self.__get_matching_cols__(argcols)
        valcols = self.__get_matching_cols__(valcols)

        def __arg_max_grouping_func__(mps):
            # initialize
            max_keys = []
            for i in range(len(argcols)):
                max_keys.append([])
            max_values = []
            max_str_values = []
            for i in range(len(valcols)):
                max_str_values.append("")
                if (use_string_datatype == False):
                    max_values.append(sign * float('-inf'))
                else:
                    max_values.append("")

            # iterate over all values
            for mp in mps:
                # read keys
                keys = []
                for i in range(len(argcols)):
                    keys.append(mp[argcols[i]])

                # read values. TODO: dont do float conversion here as it distorts original data
                values = []
                str_values = []
                for i in range(len(valcols)):
                    str_values.append(str(mp[valcols[i]]))
                    if (use_string_datatype == False):
                        values.append(sign * float(mp[valcols[i]]))
                    else:
                        values.append(str(mp[valcols[i]]))

                # check if a new max has been found
                found = False
                for i in range(len(values)):
                    if (max_values[i] < values[i]):
                        # found a new max
                        for j in range(len(keys)):
                            max_keys[j] = [str(keys[j])]
                        for j in range(len(values)):
                            max_values[j] = values[j]
                            max_str_values[j] = str_values[j]
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
                result[valcols[i] + ":val" + str(i+1)] = str(max_str_values[i])

            # return
            return result

        # combine both columns
        combined_cols = []
        for k in argcols:
            combined_cols.append(k)
        for k in valcols:
            combined_cols.append(k)

        # remaining validation done by the group_by_key
        return self.group_by_key(grouping_cols, combined_cols, __arg_max_grouping_func__, suffix = suffix, collapse = collapse, dmsg = dmsg)

    # TODO: this use_string_datatype is temporary and needs to be replaced with better design.
    def aggregate(self, grouping_col_or_cols, agg_cols, agg_funcs, collapse = True, precision = None, use_rolling = None, use_string_datatype = None,
	string_datatype_cols = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "aggregate")

        # check empty
        if (self.has_empty_header()):
            raise Exception("aggregate: empty tsv")

        # check for usage of builtin functions
        for agg_func in agg_funcs:
            # raise warning if builtin functions are used
            if (__is_builtin_func__(agg_func)):
                utils.warn("aggregate: builtin function used that has side effect. Please use funclib.* functions")

        # both use_string_datatype and string_datatype_cols are deprecated
        if (use_string_datatype is not None or string_datatype_cols is not None):
            utils.warn("aggregate: use_string_datatype and string_datatype_cols are deprecated")

        # validation on precision
        if (precision is not None):
            raise Exception("aggregate: precision parameter is deprecated")

        # validation on use_rolling 
        if (use_rolling is not None or use_rolling == True):
            raise Exception("aggregate: use_rolling parameter is deprecated")

        # get matching columns
        grouping_cols = self.__get_matching_cols__(grouping_col_or_cols)

        # validation on number of grouping cols
        if (len(grouping_cols) == 0 or len(agg_cols) == 0):
            raise Exception("No input columns: {}, {}, {}".format(grouping_cols, agg_cols, suffix))

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
                raise Exception("Column not found: {}, header: {}".format(agg_col, self.header_fields))

            new_cols.append(agg_col + ":" + get_func_name(agg_funcs[i]))

        # check for empty data
        if (self.num_rows() == 0):
            utils.debug("aggregate: no data. Returning new header only")

        # take the indexes
        agg_col_indexes = []
        str_agg_col_indexes = []

        # for each agg col, add the index
        for i in range(len(agg_cols)):
            agg_col = agg_cols[i]
            agg_index = self.header_map[agg_col]
            agg_col_indexes.append(agg_index)

            # check if string is to be used
            if (use_string_datatype == True and string_datatype_cols is not None and agg_col in string_datatype_cols):
                str_agg_col_indexes.append(agg_index)

        # create a map to store array of values
        value_map_arr = [{} for i in range(len(agg_col_indexes))]

        # iterate over the data
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[1/2] building groups", dmsg, counter, self.num_rows())

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

                value_map_arr[j][cols_key].append(str(fields[agg_col_indexes[j]]))

        # compute the aggregation
        value_func_map_arr = [{} for i in range(len(agg_col_indexes))]

        # for each possible index, do aggregation
        for j in range(len(agg_col_indexes)):
            for k, vs in value_map_arr[j].items():
                value_func_map_arr[j][k] = agg_funcs[j](vs)

        # create new header and data
        new_header = "\t".join(utils.merge_arrays([self.header_fields, new_cols]))
        new_data = []

        # for each output line, attach the new aggregate value
        counter = 0
        cols_key_map = {}
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[2/2] calling function", dmsg, counter, self.num_rows())

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

        # create result
        result_xtsv = TSV(new_header, new_data)
        if (collapse == True):
            # uniq cols
            uniq_cols = []
            for col in grouping_cols:
                uniq_cols.append(col)
            for new_col in new_cols:
                uniq_cols.append(new_col)
            result_xtsv = result_xtsv.select(uniq_cols)

        # return
        return result_xtsv

    def filter(self, cols, func, include_cond = True, use_array_notation = False, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "filter")

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
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[1/1] calling function", dmsg, counter, self.num_rows())

            fields = line.split("\t")
            col_values = []
            for index in indexes:
                col_values.append(fields[index])

            # use_array_notation
            if (use_array_notation == False):
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
                elif (num_cols == 11):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10])
                elif (num_cols == 12):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11])
                elif (num_cols == 13):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11], col_values[12])
                elif (num_cols == 14):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11], col_values[12], col_values[13])
                elif (num_cols == 15):
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11], col_values[12], col_values[13], col_values[14])
                else:
                    raise Exception("Number of columns is not supported beyond 10: {}".format(str(cols)))
            else:
                result = func(col_values)


            if (result == include_cond):
                new_data.append(line)

        # return
        return TSV(self.header, new_data)

    def exclude_filter(self, cols, func, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "exclude_filter")
        return self.filter(cols, func, include_cond = False, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def any_col_with_cond_exists_filter(self, cols, func, ignore_if_missing = False, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("any_col_with_cond_exists_filter: empty tsv", ignore_if_missing)
            return self

        # find the matching cols and indexes
        matching_cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)
        indexes = self.__get_col_indexes__(matching_cols)

        # check if there were any matching columns
        if (len(matching_cols) == 0):
            utils.raise_exception_or_warn("any_col_with_cond_exists_filter: no matching columns", ignore_if_missing)
            return self

        # print which columns are going to be transformed
        if (len(matching_cols) != len(cols) and len(matching_cols) != 1):
            utils.debug("any_col_with_cond_exists_filter: list of columns that will be checked: {}".format(str(matching_cols)))

        # iterate
        new_data = []
        for line in self.get_data():
            # get fields
            fields = line.split("\t", -1)

            # iterate over matching cols
            flag = False 
            for i in indexes:
                # append to new data if any column matched
                if (func(fields[i]) == True):
                    flag = True
                    break

            # check if all conditions met
            if (flag == True):
                new_data.append(line)

        # return
        return TSV(self.get_header(), new_data)
 
    def all_cols_with_cond_exists_filter(self, cols, func, ignore_if_missing = False, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("all_cols_with_cond_exists_filter: empty tsv", ignore_if_missing)
            return self

        # find the matching cols and indexes
        matching_cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)
        indexes = self.__get_col_indexes__(matching_cols)

        # check if there were any matching columns
        if (len(matching_cols) == 0):
            utils.raise_exception_or_warn("any_col_with_cond_exists_filter: no matching columns", ignore_if_missing)
            return self

        # print which columns are going to be transformed
        if (len(matching_cols) != len(cols) and len(matching_cols) != 1):
            utils.debug("any_col_with_cond_exists_filter: list of columns that will be checked: {}".format(str(matching_cols)))

        # iterate
        new_data = []
        for line in self.get_data():
            # get fields
            fields = line.split("\t", -1)

            # iterate over matching cols
            flag = True
            for i in indexes:
                # append to new data if any column matched
                if (func(fields[i]) == False):
                    flag = False
                    break

            # check if all conditions met
            if (flag == True):
                new_data.append(line)

        # return
        return TSV(self.get_header(), new_data) 
        
    def transform(self, cols, func, new_col_or_cols, use_array_notation = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transform")

        # check empty
        if (self.has_empty_header()):
            raise Exception("transform: empty header tsv")

        # resolve to matching_cols
        matching_cols = self.__get_matching_cols__(cols)

        # find if the new cols is a single value or array
        new_cols = None
        if (isinstance(new_col_or_cols, str)):
            new_cols = [new_col_or_cols]
        else:
            new_cols = new_col_or_cols

        # number of new_cols
        num_new_cols = len(new_cols)

        # validation
        if ((utils.is_array_of_string_values(cols) == False and len(matching_cols) != 1) or (utils.is_array_of_string_values(cols) == True and len(matching_cols) != len(cols))):
            raise Exception("transform api doesnt support regex style cols array as the order of columns matter: {}, {}".format(cols, matching_cols))

        # validation
        for col in matching_cols:
            if (col not in self.header_map.keys()):
                raise Exception("Column: {} not found in {}".format(str(col), str(self.get_header_fields())))

        # new col validation
        for new_col in new_cols:
            if (new_col in self.header_fields):
                raise Exception("New column: {} already exists in {}".format(new_col, str(self.get_header_fields())))

        # check for no data
        if (self.num_rows() == 0):
            # just add new columns and return
            return self.add_empty_cols_if_missing(new_cols)

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
        for line in self.get_data():
            counter = counter + 1
            utils.report_progress("[1/1] calling function", dmsg, counter, self.num_rows())

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
                    result = func(col_values[0], col_values[1], col_values[2], col_values[3], col_values[4], col_values[5], col_values[6], col_values[7], col_values[8], col_values[9], col_values[10], col_values[11], col_values[12], col_values[13], col_values[14])
                else:
                    raise Exception("Number of columns is not supported beyond 15. Probably try to use use_array_notation approach: {}".format(str(cols)))
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
                    raise Exception("Number of new columns is not supported beyond 10. Probably try to use use_array_notation approach: {}".format(str(new_cols)))
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

    def transform_inline(self, cols, func, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transform_inline")

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
            utils.trace("transform_inline: list of columns that will be transformed: {}".format(str(matching_cols)))

        # create new data
        new_data = []
        counter = 0
        for line in self.get_data():
            counter = counter + 1
            utils.report_progress("[1/1] calling function", dmsg, counter, self.num_rows())

            fields = line.split("\t")
            new_fields = []
            for i in range(len(fields)):
                if (i in indexes):
                    new_fields.append(str(func(fields[i])))
                else:
                    new_fields.append(str(fields[i]))

            new_data.append("\t".join(new_fields))

        # return
        return TSV(self.header, new_data)

    def transform_inline_log(self, col_or_cols, base = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transform_inline_log")
        if (base is None):
            return self.transform_inline(col_or_cols, lambda t: math.log(float(t)), dmsg = dmsg)
        elif (base == 2):
            return self.transform_inline(col_or_cols, lambda t: math.log2(float(t)), dmsg = dmsg)
        elif (base == 10):
            return self.transform_inline(col_or_cols, lambda t: math.log10(float(t)), dmsg = dmsg)
        else:
            raise Exception("transform_inline_log: base value is not supported: {}".format(base))

    def transform_inline_log2(self, col_or_cols, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transform_inline_log2")
        return self.transform_inline_log(col_or_cols, base = 2, dmsg = dmsg)

    def transform_inline_log10(self, col_or_cols, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transform_inline_log10")
        return self.transform_inline_log(col_or_cols, base = 10, dmsg = dmsg)

    def transform_inline_log1p(self, col_or_cols, base = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transform_inline_log1p")
        if (base is None):
            return self.transform_inline(col_or_cols, lambda t: math.log(1 + float(t)), dmsg = dmsg)
        elif (base == 2):
            return self.transform_inline(col_or_cols, lambda t: math.log2(1 + float(t)), dmsg = dmsg)
        elif (base == 10):
            return self.transform_inline(col_or_cols, lambda t: math.log10(1 + float(t)), dmsg = dmsg)
        else:
            raise Exception("transform_inline_log1p: base value is not supported: {}".format(base))

    def transform_inline_log1p_base10(self, col_or_cols, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transform_inline_log1p_base10")
        return self.transform_inline_log1p(col_or_cols, base = 10, dmsg = dmsg)
        
    def rename(self, col, new_col, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("rename: empty tsv")

        # validation
        if (self.has_col(col) == False):
            raise Exception("Column: {} not found in {}".format(str(col), str(self.get_header_fields())))

        # validation
        if (self.has_col(new_col) == True):
            raise Exception("New Column: {} already exists in {}".format(str(new_col), str(self.get_header_fields())))

        # new header fields
        new_header_fields = list([new_col if (h == col) else h for h in self.get_header_fields()])
        new_header = "\t".join(new_header_fields)

        # return 
        return TSV(new_header, self.get_data())

    def get_header(self):
        return self.header

    def get_data(self):
        return self.data

    def get_header_map(self):
        return self.header_map

    def num_rows(self):
        return len(self.get_data())

    def num_cols(self):
        return len(self.get_header_fields())

    def get_size_in_bytes(self):
        utils.warn("Please use size_in_bytes() instead")
        return self.size_in_bytes()

    def size_in_bytes(self):
        total = len(self.header)
        for line in self.get_data():
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

        # create a map for each row
        for line in self.get_data():
            fields = line.split("\t")
            mp = {}
            for i in range(len(self.get_header_fields())):
                mp[self.header_fields[i]] = str(fields[i])
            mps.append(mp)

        # return
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

    def to_int(self, cols, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "to_int")
        return self.transform_inline(cols, lambda x: int(float(x)), dmsg = dmsg)

    # TODO
    def to_numeric(self, cols, precision = 6, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "to_numeric")
        return self.transform_inline(cols, lambda x: self.__convert_to_numeric__(x, precision), dmsg = dmsg)

    def add_seq_num(self, new_col, start = 1, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("add_seq_num: empty tsv")
            return self

        # validation
        if (new_col in self.header_map.keys()):
            raise Exception("Output column name: {} already exists in {}".format(new_col, self.header_fields))

        # create new header
        new_header = new_col + "\t" + self.header

        # create new data
        new_data = []
        counter = start - 1 
        for line in self.get_data():
            counter = counter + 1
            utils.report_progress("add_seq_num: [1/1] adding new column", dmsg, counter, self.num_rows())
            new_data.append(str(counter) + "\t" + line)

        # return
        return TSV(new_header, new_data)

    def show_transpose(self, n = 1, title = "Show Transpose", max_col_width = None, debug_only = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "show_transpose")

        # check empty
        if (self.has_empty_header()):
            return self

        # check debug_only flag
        if (debug_only == True and utils.is_debug() == False):
            return self

        # validation and doing min
        if (self.num_rows() < n):
            n = self.num_rows()

        # max width of screen to determine per column width
        if (max_col_width is None):
            max_width = 180
            max_col_width = int(max_width / (n + 1))

        # print
        self \
            .__show_title_header__(title) \
            .transpose(n, dmsg = dmsg) \
            .show(n = self.num_cols(), title = None, max_col_width = max_col_width, dmsg = dmsg) \
            .__show_title_footer__(title)

        # return self
        return self

    def show(self, n = 100, title = "Show", max_col_width = 40, debug_only = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "show")

        # check empty
        if (self.has_empty_header()):
            return self

        # check debug_only flag
        if (debug_only == True and utils.is_debug() == False):
            return self

        # show topn
        self \
            .__show_title_header__(title) \
            .take(n, dmsg = dmsg) \
            .__show_topn__(max_col_width) \
            .__show_title_footer__(title)

        # return the original tsv
        return self

    def show_sample(self, n = 100, title = None, max_col_width = 40, debug_only = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "show_sample")

        # check empty
        if (self.has_empty_header()):
            return self

        # check debug_only flag
        if (debug_only == True and utils.is_debug() == False):
            return self

        # show topn
        self \
            .__show_title_header__(title) \
            .sample_n(n, dmsg = dmsg) \
            .__show_topn__(max_col_width) \
            .__show_title_footer__(title)

        # return
        return self

    def __show_title_header__(self, title):
        # print label
        if (title is not None):
            print("=============================================================================================================================================")
            print("Title: {}, Num Rows: {}, Num Cols: {}".format(title, self.num_rows(), self.num_cols()))
            print("=============================================================================================================================================")

        # return
        return self

    def __show_title_footer__(self, title):
        # check for title
        if (title is not None):
            print("=============================================================================================================================================")

        # print blank
        print("")

        # return
        return self

    def __show_topn__(self, max_col_width):
        spaces = " ".join([""]*max_col_width)

        # gather data about width of columns
        col_widths = {}
        is_numeric_type_map = {}

        # determine which columns are numeric type
        for k in self.header_map.keys():
            col_widths[k] = min(len(k), max_col_width)
            is_numeric_type_map[k] = True

        # determine width
        for line in self.get_data():
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
        for line in self.get_data():
            all_data.append(line)

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

        # print blank lines
        print("")

        # return self
        return self

    def col_as_array(self, col):
        # check empty
        if (self.has_empty_header()):
            raise Exception("col_as_array: empty tsv")

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        index = self.header_map[col]
        ret_values = []
        for line in self.get_data():
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
        utils.debug_once("[OLD_WARN]: cols_as_map: This api has changed from prev implementation")

        # validation
        key_cols = self.__get_matching_cols__(key_cols)

        # check for all columns in the value part
        value_cols = self.__get_matching_cols__(value_cols)

        # Change in criteria. This api is confusing and for now restrict to single key and value
        if (len(key_cols) > 1):
            raise Exception("cols_as_map: using key_cols as more than 1 column is deprecated: {}".format(key_cols)) 

        if (len(value_cols) > 1):
            raise Exception("cols_as_map: using value_cols as more than 1 column is deprecated: {}".format(value_cols)) 

        # create map
        mp = {}
        for line in self.get_data():
            fields = line.split("\t")
            # get the key
            keys = []
            for key_col in key_cols:
                key = fields[self.header_map[key_col]]
                keys.append(key)
            keys_tuple = self.__expand_to_tuple__(keys)

            # check for non duplicate keys
            if (keys_tuple in mp.keys()):
                raise Exception("keys is not unique: {}".format(keys))

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
                values.append(str(fields[i]))

        return tuple(values)

    # TODO: this api needs to remove the auto detection of all_numeric flag
    def sort(self, cols = None, reverse = False, reorder = False, all_numeric = None, ignore_if_missing = False, dmsg = ""):
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
        dmsg = utils.extend_inherit_message(dmsg, "sort")
        if (reorder == True):
            return TSV(self.header, new_data).reorder(matching_cols, dmsg = dmsg)
        else:
            return TSV(self.header, new_data)

    def reverse_sort(self, cols = None, reorder = False, all_numeric = None, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "reverse_sort")
        return self.sort(cols = cols, reverse = True, reorder = reorder, all_numeric = all_numeric, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def numerical_sort(self, cols = None, reorder = False, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "numerical_sort")
        return self.sort(cols = cols, reorder = reorder, all_numeric = True, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def reverse_numerical_sort(self, cols = None, reorder = False, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "reverse_numerical_sort")
        return self.reverse_sort(cols = cols, reorder = reorder, all_numeric = True, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    # reorder the specific columns
    def reorder(self, cols, use_existing_order = False, dmsg = ""):
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
                all_cols.append(c)
            for c in non_matching_cols:
                all_cols.append(c)

            # return
            return self.select(all_cols).reorder(cols, use_existing_order = True, dmsg = dmsg)

        # create a map of columns that match the criteria
        new_header_fields = []

        # append all the matching columns
        for h in self.get_header_fields():
            if (h in matching_cols):
                new_header_fields.append(h)

        # append all the remaining columns
        for h in self.get_header_fields():
            if (h not in matching_cols):
                new_header_fields.append(h)

        # pass on the message
        dmsg = utils.extend_inherit_message(dmsg, "reorder")
        return self.select(new_header_fields, dmsg = dmsg)

    def reorder_reverse(self, cols, dmsg = ""):
        utils.warn("Please use reverse_reorder instead")
        return self.reverse_reorder(cols, dmsg)

    # reorder for pushing the columns to the end
    def reverse_reorder(self, cols, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("reorder: empty tsv")

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(cols)

        # generate the list of cols that should be brought to front
        rcols = []
        for h in self.get_header_fields():
            if (h not in matching_cols):
                rcols.append(h)

        # pass on the message
        dmsg = utils.extend_inherit_message(dmsg, "reorder_reverse")
        return self.reorder(rcols, dmsg = dmsg)

    def noop(self, *args, **kwargs):
        return self

    def to_df(self, n = None, infer_data_types = True, no_infer_cols = None):
        # find how many rows to select
        nrows = len(self.get_data()) if (n is None) else n

        # validation
        if (nrows < 0):
            raise Exception("to_df(): n can not be negative: {}".format(nrows))

        # initialize map
        df_map = {}
        for h in self.get_header_fields():
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
                elif (self.__has_all_float_values__(col)):
                    float_cols.append(col)

        # iterate over data
        for line in self.data[0:nrows]:
            fields = line.split("\t")

            # iterate over fields
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

    def to_simple_df(self, n = None):
        return self.to_df(n = n, infer_data_types = False)

    def export_to_df(self, n = -1):
        utils.warn("Deprecated. Use to_df()")
        return self.to_df(n)

    def to_json_records(self, new_col = "json"):
        new_header = new_col
        new_data = []

        # new col validation
        if (new_col in self.header_map.keys()):
            raise Exception("New column: {} already exists in {}".format(new_col, str(self.get_header_fields())))

        # iterate
        for line in self.get_data():
            fields = line.split("\t")
            mp = {}
            for i in range(len(self.get_header_fields())):
                mp[self.header_fields[i]] = fields[i]
            new_data.append(json.dumps(mp))

        return TSV(new_header, new_data)

    def to_csv(self, comma_replacement = ";"):
        utils.warn("to_csv: This is not a standard csv conversion. The commas are just replaced with another character specified in comma_replacement parameter.")

        # create new data
        new_header = self.header.replace(",", comma_replacement).replace("\t", ",")
        new_data = []
        for line in self.get_data():
            line = line.replace(",", comma_replacement).replace("\t", ",")
            new_data.append(line)

        # still return as tsv with single column that is special
        return TSV(new_header, new_data)

    def url_encode_inline(self, col_or_cols, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "url_encode_inline")
        return self.transform_inline(col_or_cols, lambda x: utils.url_encode(x), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def url_decode_inline(self, col_or_cols, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "url_decode_inline")
        return self.transform_inline(col_or_cols, lambda x: utils.url_decode(x), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def url_decode_clean_inline(self, col_or_cols, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "url_decode_clean_inline")
        return self.transform_inline(col_or_cols, lambda x: utils.url_decode_clean(x), ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def url_encode(self, col, newcol, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "url_encode")
        return self.transform([col], lambda x: utils.url_encode(x), newcol, dmsg = dmsg)

    def url_decode(self, col, newcol, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "url_decode")
        return self.transform([col], lambda x: utils.url_decode(x), newcol, dmsg = dmsg)

    def resolve_url_encoded_cols(self, suffix = "url_encoded", ignore_if_missing = True, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "resolve_url_encoded_cols")
        return self \
            .url_decode_inline(".*:{}".format(suffix), ignore_if_missing = ignore_if_missing, dmsg = dmsg) \
            .remove_suffix(suffix, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    def union(self, tsv_or_that_arr):
        # check if this is a single element TSV or an array
        if (type(tsv_or_that_arr) == TSV):
            that_arr = [tsv_or_that_arr]
        else:
            that_arr = tsv_or_that_arr

        # pick the ones with non zero rows
        that_arr = list(filter(lambda t: t.num_rows() > 0, that_arr))

        # boundary condition
        if (len(that_arr) == 0):
            return self

        # check empty
        if (self.has_empty_header()):
            # if there are multiple tsvs
            if (len(that_arr) > 1):
                return that_arr[0].union(that_arr[1:])
            else:
                return that_arr[0]

        # validation
        for that in that_arr:
            if (self.get_header() != that.get_header()):
                raise Exception("Headers are not matching for union: {}, {}".format(self.header_fields, that.get_header_fields()))

        # create new data
        new_data = []
        for line in self.get_data():
            fields = line.split("\t")
            if (len(fields) != len(self.get_header_fields())):
                raise Exception("Invalid input data. Fields size are not same as header: header: {}, fields: {}".format(self.header_fields, fields))
            new_data.append(line)

        for that in that_arr:
            for line in that.get_data():
                fields = line.split("\t")
                if (len(fields) != len(self.get_header_fields())):
                    raise Exception("Invalid input data. Fields size are not same as header: header: {}, fields: {}".format(self.header_fields, fields))
                new_data.append(line)

        return TSV(self.header, new_data)

    # this method finds the set difference between this and that. if cols is None, then all columns are taken
    # TODO: hash collision
    def difference(self, that, cols = None, seed = 0):
        # print some warning for api that is still under development
        utils.warn("difference: to be used where duplicate rows will be removed")
        utils.warn("difference method uses hash that is not handling hash collisions")

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
        hash_tsv1 = self.generate_key_hash(cols1, temp_col, seed = seed)
        hash_tsv2 = that.generate_key_hash(cols2, temp_col, seed = seed)

        # remove entries from this where the hashes exist in that
        return hash_tsv1 \
            .values_not_in(temp_col, hash_tsv2.col_as_array_uniq(temp_col)) \
            .drop_cols(temp_col)

    def add_const(self, col, value, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            # checking empty value
            if (value == ""):
                utils.warn("add_const: empty header and empty data. extending just the header")
                return new_with_cols([col])
            else:
                raise Exception("add_const: empty header but non empty data: {}".format(value))

        # return
        dmsg = utils.extend_inherit_message(dmsg, "add_const")
        return self.transform([self.header_fields[0]], lambda x: str(value), col, dmsg = dmsg)

    def add_const_if_missing(self, col, value, dmsg = ""):
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
            dmsg = utils.extend_inherit_message(dmsg, "add_const_if_missing")
            return self.add_const(col, value, dmsg = dmsg)

    def add_empty_cols_if_missing(self, col_or_cols, prefix = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "add_empty_cols_if_missing")

        # check if this is a single col name or an array
        is_array = utils.is_array_of_string_values(col_or_cols)

        # convert to array format
        cols = col_or_cols if (is_array == True) else [col_or_cols]

        # check for prefix
        if (prefix is not None):
            cols = list(["{}:{}".format(prefix, t) for t in cols])

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
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[1/1] calling function", dmsg, counter, self.num_rows())

            # create new line
            new_line = "\t".join([line, empty_row])
            new_data.append(new_line)

        # return
        return TSV(new_header, new_data)

    def add_row(self, row_fields):
        # check empty
        if (self.has_empty_header()):
            raise Exception("{}: add_row: empty tsv".format(dmsg))

        # validation
        if (len(row_fields) != self.num_cols()):
            raise Exception("{}: Number of fields is not matching with number of columns: {} != {}".format(dmsg, len(row_fields), self.num_cols()))

        # create new row
        new_line = "\t".join(row_fields)

        # remember to do deep copy
        new_data = []
        for line in self.get_data():
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
                raise Exception("Column not in existing data: {}, {}".format(k, str(self.get_header_fields())))

        # check for default values
        for h in self.get_header_fields():
            if (h not in mp.keys()):
                if (default_val is None):
                    raise Exception("Column not present in map and default value is not defined: {}. Try using default_val".format(h))

        # add the map as new row
        new_fields = []
        for h in self.get_header_fields():
            if (h in mp.keys()):
                # append the value
                new_fields.append(utils.strip_spl_white_spaces(mp[h]))
            else:
                # append default value
                new_fields.append(default_val)

        # create new row
        return self.add_row(new_fields)

    def assign_value(self, col_or_cols, value, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "assign_value")
        return self.transform_inline(col_or_cols, lambda x: value, dmsg = dmsg)

    def concat_as_cols(self, that, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("concat_as_cols: empty tsv")
            return that

        # validation
        if (self.num_rows() != that.num_rows()):
            raise Exception("Mismatch in number of rows: {}, {}".format(self.num_rows(), that.num_rows()))

        # check for complete disjoint set of columns
        for h in self.header_map.keys():
            if (h in that.header_map.keys()):
                raise Exception("The columns for doing concat need to be completely separate: {}, {}".format(h, self.header_fields))

        # create new header
        new_header_fields = []
        for h in self.header.split("\t"):
            new_header_fields.append(h)
        for h in that.header.split("\t"):
            new_header_fields.append(h)

        new_header = "\t".join(new_header_fields)

        # create new data
        new_data = []
        for i in range(len(self.get_data())):
            line1 = self.data[i]
            line2 = that.data[i]

            new_data.append(line1 + "\t" + line2)

        return TSV(new_header, new_data)

    def add_col_prefix(self, cols, prefix, dmsg = ""):
        utils.warn("Deprecated: Use add_prefix instead")
        return self.add_prefix(self, prefix, cols)

    def remove_suffix(self, suffix, prefix = None, ignore_if_missing = False, dmsg = ""):
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
        for c in self.get_header_fields():
            if (c.endswith(suffix)):
                # check if the prefix is defined and matcing
                if (prefix is None or c.startswith(prefix + ":") == True):
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

    def add_prefix(self, prefix, cols = None, ignore_if_missing = False, dmsg = ""):
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
        for h in self.get_header_fields():
            if (h in cols):
                new_header_fields.append(prefix + ":" + h)
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def add_suffix(self, suffix, cols = None, ignore_if_missing = False, dmsg = ""):
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
        for h in self.get_header_fields():
            if (h in cols):
                new_header_fields.append(h + ":" + suffix)
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def rename_prefix(self, old_prefix, new_prefix, cols = None, ignore_if_missing = False, dmsg = ""):
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
        for h in self.get_header_fields():
            if (h in cols):
                new_header_fields.append(new_prefix + h[len(old_prefix):])
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def rename_suffix(self, old_suffix, new_suffix, cols = None, ignore_if_missing = False, dmsg = ""):
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
        for h in self.get_header_fields():
            if (h in cols):
                new_header_fields.append(h[0:-1*len(old_suffix) + new_suffix])
            else:
                new_header_fields.append(h)

        # return
        return TSV("\t".join(new_header_fields), self.data)

    def remove_prefix(self, prefix, ignore_if_missing = False, dmsg = ""):
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
        for c in self.get_header_fields():
            if (c.startswith(prefix)):
                new_col = c[len(prefix):]
                if (new_col in self.get_header_fields() or len(new_col) == 0):
                    raise Exception("Duplicate names. Cant do the prefix: {}, {}, {}".format(c, new_col, str(self.get_header_fields())))
                mp[c] = new_col

        # validation
        if (len(mp) == 0):
            utils.raise_exception_or_warn("prefix didnt match any of the columns: {}, {}".format(prefix, str(self.get_header_fields())), ignore_if_missing)

        new_header = "\t".join(list([h if (h not in mp.keys()) else mp[h] for h in self.get_header_fields()]))
        return TSV(new_header, self.data)

    def sample(self, sampling_ratio, seed = 0, with_replacement = False, dmsg = ""):
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
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("sample: [1/1] calling function", dmsg, counter, self.num_rows())

            # this random number is only for basic sampling and not for doing anything sensitive.
            if (random.random() <= sampling_ratio):  # nosec
                new_data.append(line)

        return TSV(self.header, new_data)

    def sample_without_replacement(self, sampling_ratio, seed = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "sample_without_replacement")
        return self.sample(sampling_ratio, seed, with_replacement = False, dmsg = dmsg)

    def sample_with_replacement(self, sampling_ratio, seed = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "sample_with_replacement")
        return self.sample(sampling_ratio, seed, with_replacement = True, dmsg = dmsg)

    def sample_rows(self, n, seed = 0, dmsg = ""):
        utils.warn("Please use sample_n")
        dmsg = utils.extend_inherit_message(dmsg, "sample_rows")
        return self.sample_n(n, seed, dmsg = dmsg)

    def sample_n(self, n, seed = 0, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_n: empty tsv")
            return self

        # validation
        if (n < 0):
            raise Exception("n cant be negative: {}".format(n))

        # check if n == 0
        if (n == 0):
            return self.take(0)

        # set seed
        random.seed(seed)
        n = min(int(n), self.num_rows())

        # sample and return. the debug message is not in standard form, but its fine.
        utils.report_progress("sample_n: [1/1] calling function", dmsg, len(self.get_data()), len(self.get_data()))
        return TSV(self.header, random.sample(self.data, n))

    # TODO: WIP
    def sample_n_with_warn(self, limit, msg = None, seed = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "sample_n_with_warn")
        utils.warn_once("sample_n_with_warn: this api name might change")

        # check if input exceeds the max limit
        if (self.num_rows() > limit):
            if (msg is not None):
                utils.warn("{}: {}, num_rows: {}, limit: {}".format(dmsg, msg, self.num_rows(), limit))
            else:
                utils.warn("{}: Input exceeds the limit. {} > {}. Taking a sample".format(dmsg, self.num_rows(), limit))

            # return
            return self \
                .sample_n(limit, seed = seed, dmsg = dmsg)
        else:
            return self

    def sample_group_by_topk_if_reached_limit(self, limit, *args, **kwargs):
        utils.warn_once("sample_group_by_topk_if_reached_limit: this api name might change")

        # check if sampling is needed
        if (self.num_rows() > limit):
            return self \
                .sample_group_by_topk(*args, **kwargs)
        else:
            return self 

    def warn_if_limit_reached(self, limit, msg = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "warn_if_limit_reached")
        utils.warn_once("warn_if_limit_reached: this api name might change")

        # check for limit
        if (self.num_rows() >= limit):
            if (msg is not None):
                utils.warn("{}: {}, num_rows: {}, limit: {} reached".format(dmsg, msg, self.num_rows(), limit))
            else:
                utils.warn("{}: num_rows: {}, limit: {} reached".format(dmsg, self.num_rows(), limit))

        # return
        return self

    def cap_min_inline(self, col, value, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "cap_min_inline")
        return self.transform_inline([col], lambda x: str(x) if (float(value) < float(x)) else str(value), dmsg = dmsg)

    def cap_max_inline(self, col, value, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "cap_max_inline")
        return self.transform_inline(col, lambda x: str(value) if (float(value) < float(x)) else str(x), dmsg = dmsg)

    def cap_min(self, col, value, newcol, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "cap_min")
        return self.transform_inline(col, lambda x: str(x) if (float(value) < float(x)) else str(value), newcol, dmsg = dmsg)

    def cap_max(self, col, value, newcol, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "cap_max")
        return self.transform([col], lambda x: str(value) if (float(value) < float(x)) else str(x), newcol, dmsg = dmsg)

    def copy(self, col, newcol, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "copy")
        return self.transform([col], lambda x: x, newcol, dmsg = dmsg)

    # sampling method to do class rebalancing where the class is defined by a specific col-value. As best practice,
    # the sampling ratios should be determined externally.
    def sample_class(self, col, col_value, sampling_ratio, seed = 0, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_class: empty tsv")
            return self

        # Validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        # cap the sampling ratio to 1
        if (sampling_ratio > 1):
            sampling_ratio = 1.0

        # set the seed
        random.seed(seed)

        # resample
        new_data = []
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("sample_class: [1/1] calling function", dmsg, counter, self.num_rows())

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
    def sample_group_by_col_value(self, grouping_cols, col, col_value, sampling_ratio, seed = 0, use_numeric = False, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_col_value: empty tsv")
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        # check sampling ratio
        if (sampling_ratio < 0 or sampling_ratio > 1):
            raise Exception("Sampling ratio has to be between 0 and 1: {}".format(sampling_ratio))

        # group by and apply the sampling on the value. The assumption is that all rows in the same group should have the same col_value
        dmsg = utils.extend_inherit_message(dmsg, "sample_group_by_col_value")
        agg_result = self \
            .aggregate(grouping_cols, [col], [self.__sample_group_by_col_value_agg_func__(col_value, sampling_ratio, seed, use_numeric)], collapse = False, dmsg = utils.extend_inherit_message(dmsg, "[1/3]")) \
            .values_in("{}:__sample_group_by_col_value_agg_func_inner__".format(col), ["1"], dmsg = utils.extend_inherit_message(dmsg, "[2/3]")) \
            .drop_cols("{}:__sample_group_by_col_value_agg_func_inner__".format(col), dmsg = utils.extend_inherit_message(dmsg, "[3/3]"))

        # return
        return agg_result

    # TODO: this is using comma as join. can get buggy
    def __sample_group_by_max_uniq_values_exact_group_by__(self, k, n, seed):
        def __sample_group_by_max_uniq_values_exact_group_by_inner__(mps):
            vs = []
            for mp in mps:
                vs.append(mp[k])
            
            # do a unique and shuffle
            vs_uniq = utils.random_shuffle(list(set(vs)), seed = seed)
            vs_selected = vs_uniq[0:int(min(n, len(vs_uniq)))]

            # create result
            result_mp = {}
            result_mp["found"] = ",".join(vs_selected)
                    
            # return
            return result_mp
        
        return __sample_group_by_max_uniq_values_exact_group_by_inner__

    def sample_group_by_max_uniq_values_exact(self, grouping_cols, col, max_uniq_values, seed = 0, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_max_uniq_values_exact: empty tsv")
            return self

        # check for no data
        if (self.num_rows() == 0):
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        # check max_uniq_values
        if (max_uniq_values <= 0):
            raise Exception("max_uniq_values has to be more than 0: {}".format(max_uniq_values))

        # agg result
        dmsg = utils.extend_inherit_message(dmsg, "sample_group_by_max_uniq_values_exact")

        # compute
        agg_result = self \
            .group_by_key(grouping_cols, col, self.__sample_group_by_max_uniq_values_exact_group_by__(col, max_uniq_values, seed), suffix = "__sample_group_by_max_uniq_values_exact_group_by__",
                collapse = False, dmsg = utils.extend_inherit_message(dmsg, "[1/3]"))  \
            .filter([col, "found:__sample_group_by_max_uniq_values_exact_group_by__"], lambda x,y: x in y.split(","), dmsg = utils.extend_inherit_message(dmsg, "[2/3]")) \
            .drop_cols("found:__sample_group_by_max_uniq_values_exact_group_by__", dmsg = utils.extend_inherit_message(dmsg, "[3/3]"))

        # return
        return agg_result

    def __sample_group_by_max_uniq_values_approx_uniq_count__(self, vs):
        return len(set(vs))

    # sampling method to take a grouping key, and a column where the number of unique values for column are capped. this uses approximate sampling technique
    def sample_group_by_max_uniq_values_approx(self, grouping_cols, col, max_uniq_values, seed = 0, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_max_uniq_values_approx: empty tsv")
            return self

        # check seed
        if (seed is None or seed < 0):
            raise Exception("Invalid seed: {}".format(seed))

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        # check grouping cols
        for k in grouping_cols:
            if (k not in self.header_map.keys()):
                raise Exception("Grouping Column not found: {}, {}".format(str(k), str(self.get_header_fields())))

        # check max_uniq_values
        if (max_uniq_values <= 0):
            raise Exception("max_uniq_values has to be more than 0: {}".format(max_uniq_values))

        # the hashing function is applied on the entire grouping_cols + col
        sample_grouping_cols = [g for g in grouping_cols]
        sample_grouping_cols.append(col)

        # agg result
        dmsg = utils.extend_inherit_message(dmsg, "sample_group_by_max_uniq_values_approx")
        agg_result = self \
            .aggregate(grouping_cols, [col], [self.__sample_group_by_max_uniq_values_approx_uniq_count__], collapse = False, dmsg = utils.extend_inherit_message(dmsg, "[1/5]")) \
            .transform(["{}:__sample_group_by_max_uniq_values_approx_uniq_count__".format(col)], lambda c: max_uniq_values / float(c) if (float(c) > max_uniq_values) else 1, "{}:__sample_group_by_max_uniq_values_approx_sampling_ratio__".format(col), dmsg = utils.extend_inherit_message(dmsg, "[2/5]")) \
            .transform(sample_grouping_cols, lambda x: abs(utils.compute_hash("\t".join(x), seed)) / sys.maxsize, "{}:__sample_group_by_max_uniq_values_approx_sampling_key__".format(col), use_array_notation = True, dmsg = utils.extend_inherit_message(dmsg, "[3/5]")) \
            .filter(["{}:__sample_group_by_max_uniq_values_approx_sampling_key__".format(col), "{}:__sample_group_by_max_uniq_values_approx_sampling_ratio__".format(col)], lambda x, y: float(x) <= float(y), dmsg = utils.extend_inherit_message(dmsg, "[4/5]")) \
            .drop_cols("^{}:__sample_group_by_max_uniq_values_approx.*".format(col), dmsg = utils.extend_inherit_message(dmsg, "[5/5]"))

        # return
        return agg_result

    def sample_group_by_max_uniq_values(self, grouping_cols, col, max_uniq_values, seed = 0, use_approx = True, dmsg = ""):
        # debug message
        dmsg = utils.extend_inherit_message(dmsg, "sample_group_by_max_uniq_values_approx")

        # select the function with approximation if needed
        if (use_approx == True):
            return self.sample_group_by_max_uniq_values_approx(grouping_cols, col, max_uniq_values, seed = seed, dmsg = dmsg)
        else:
            return self.sample_group_by_max_uniq_values_exact(grouping_cols, col, max_uniq_values, dmsg = dmsg)

    def __sample_group_by_max_uniq_values_per_class_uniq_count__(self, vs):
        return len(set(vs))

    # sampling method to take a grouping key, and a column where the number of unique values for column are capped.
    def sample_group_by_max_uniq_values_per_class(self, grouping_cols, class_col, col, max_uniq_values_map, def_max_uniq_values = None , seed = 0, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_max_uniq_values_per_class: empty tsv")
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # validation
        if (col not in self.header_map.keys()):
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))
        if (class_col not in self.header_map.keys()):
            raise Exception("Column not found: {}, {}".format(str(class_col), str(self.get_header_fields())))

        # correctly define def_max_uniq_values
        if (def_max_uniq_values is None):
            def_max_uniq_values = self.num_rows()

        # validation on def_max_uniq_values
        if (def_max_uniq_values <= 0):
            raise Exception("max_uniq_values has to be more than 0: {}".format(def_max_uniq_values))

        # the hashing function is applied on the entire grouping_cols + col
        sample_grouping_cols = [g for g in grouping_cols]
        # check if class_col is already in grouping_cols
        if (class_col not in grouping_cols):
            sample_grouping_cols.append(class_col)
        else:
            utils.warn("sample_group_by_max_uniq_values_per_class: class_col need not be specified in the grouping cols: {}".format(str(grouping_cols)))
        sample_grouping_cols.append(col)

        # aggregate result
        dmsg = utils.extend_inherit_message(dmsg, "sample_group_by_max_uniq_values_per_class")
        agg_result = self \
            .aggregate(grouping_cols, [col], "", [self.__sample_group_by_max_uniq_values_per_class_uniq_count__], collapse = False, dmsg = utils.extend_inherit_message(dmsg, "[1/6]")) \
            .transform([class_col], lambda c: str(max_uniq_values_map[c]) if (c in max_uniq_values_map.keys()) else str(def_max_uniq_values), "{}:__sample_group_by_max_uniq_values_per_class_max_uniq_values__".format(col), dmsg = utils.extend_inherit_message(dmsg, "[2/6]")) \
            .transform(["{}:__sample_group_by_max_uniq_values_per_class_uniq_count__".format(col), "{}:__sample_group_by_max_uniq_values_per_class_max_uniq_values__".format(col)], lambda c, m: float(m) / float(c) if (float(c) > float(m)) else 1, "{}:__sample_group_by_max_uniq_values_per_class_sampling_ratio__".format(col), dmsg = utils.extend_inherit_message(dmsg, "[3/6]")) \
            .transform(sample_grouping_cols, lambda x: abs(utils.compute_hash("\t".join(x), seed)) / sys.maxsize, "{}:__sample_group_by_max_uniq_values_per_class_sampling_key__".format(col), use_array_notation = True, dmsg = utils.extend_inherit_message(dmsg, "[4/6]")) \
            .filter(["{}:__sample_group_by_max_uniq_values_per_class_sampling_key__".format(col), "{}:__sample_group_by_max_uniq_values_per_class_sampling_ratio__".format(col)], lambda x, y: float(x) <= float(y), dmsg = utils.extend_inherit_message(dmsg, "[5/6]")) \
            .drop_cols("^{}:__sample_group_by_max_uniq_values_per_class.*".format(col), dmsg = utils.extend_inherit_message(dmsg, "[6/6]"))

        # return
        return agg_result

    # random sampling within a group
    def sample_group_by_key(self, grouping_cols, sampling_ratio, seed = 0, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            utils.warn("sample_group_by_key: empty tsv")
            return self

        # resolve grouping_cols
        grouping_cols = self.__get_matching_cols__(grouping_cols)

        # check sampling ratio
        if (sampling_ratio < 0 or sampling_ratio > 1):
            raise Exception("Sampling ratio has to be between 0 and 1: {}".format(sampling_ratio))

        # create new data
        new_data = []
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("sample_group_by_key: [1/1] calling function", dmsg, counter, self.num_rows())

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
    def sample_column_by_max_uniq_values(self, col, max_uniq_values, seed = 0, dmsg = ""):
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
            dmsg = utils.extend_inherit_message(dmsg, "sample_column_by_max_uniq_values")
            return self.values_in(col, selected_values, dmsg = dmsg)
        else:
            utils.warn("sample_column_by_max_uniq_values: max sample size: {} more than number of uniq values: {}".format(max_uniq_values, len(uniq_values)))
            return self

    # create descriptive methods for join
    def left_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        # check for empty
        if (self.has_empty_header()):
            utils.warn("left_join: empty this tsv")
            return self

        # return
        dmsg = utils.extend_inherit_message(dmsg, "left_join")
        return self.__join__(that, lkeys, rkeys, join_type = "left", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, num_par = num_par, dmsg = dmsg)

    def right_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "right_join")

        # check for empty
        if (self.has_empty_header()):
            utils.warn("right_join: empty this tsv")
            return that

        # return
        return self.__join__(that, lkeys, rkeys, join_type = "right", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, num_par = num_par, dmsg = dmsg)

    def inner_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "inner_join")

        # check for empty
        if (self.has_empty_header()):
            raise Exception("inner_join: empty this tsv")

        # return
        return self.__join__(that, lkeys, rkeys, join_type = "inner", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, num_par = num_par, dmsg = dmsg)

    def outer_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "outer_join")

        # check for empty
        if (self.has_empty_header()):
            utils.warn("outer_join: empty this tsv")
            return that

        # return
        return self.__join__(that, lkeys, rkeys, join_type = "outer", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, num_par = num_par, dmsg = dmsg)

    def join(self, *args, **kwargs):
        utils.warn("Use the other methods: inner_join, left_join, right_join, outer_join versions of this api and not this one directly")
        return self.__join__(*args, **kwargs)

    # primary join method. Use the other inner, left, right versions and not this directly. TODO: not efficient
    def __join__(self, that, lkeys, rkeys = None, join_type = "inner", lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "__join__")

        utils.warn_once("{}: this method is not fully tested and also is very inefficient. Dont use for more than 10000 rows data".format(dmsg))
        utils.warn_once("{}: split_threshold parameter is replaced with num_par".format(dmsg))

        # matching
        lkeys = self.__get_matching_cols__(lkeys)
        rkeys = that.__get_matching_cols__(rkeys) if (rkeys is not None) else that.__get_matching_cols__(lkeys)

        # find the indexes
        lkey_indexes = []
        lvalue_indexes = []

        # relative ordering matters
        for h in lkeys:
            if (h in self.get_header_fields()):
                lkey_indexes.append(self.get_header_map()[h])

        for h in self.get_header_fields():
            if (h not in lkeys):
                lvalue_indexes.append(self.get_header_map()[h])

        rkey_indexes = []
        rvalue_indexes = []

        # relative ordering matters
        for h in rkeys:
            if (h in that.get_header_fields()):
                rkey_indexes.append(that.get_header_map()[h])

        for h in that.get_header_fields():
            if (h not in rkeys):
                rvalue_indexes.append(that.get_header_map()[h])

        # check the lengths
        if (len(lkeys) != len(rkeys)):
            raise Exception("Length mismatch in lkeys and rkeys: {}, {}".format(lkeys, rkeys))

        # print stats for left and right side
        utils.debug("{}: left num_rows: {}, right num_rows: {}".format(dmsg, self.num_rows(), that.num_rows()))

        # Check for num_par. TODO: Experimental
        if (num_par > 0):
            # split left and right sides
            left_batches = self.__split_batches_by_cols__(num_par, lkeys, dmsg = dmsg)
            right_batches = that.__split_batches_by_cols__(num_par, rkeys, dmsg = dmsg)

            # call join on individual batches and then return the merge
            tasks = []
            for i in range(num_par):
                # debug
                utils.debug("Calling join on batch: {}, left: {}, right: {}".format(i, left_batches[i].num_rows(), right_batches[i].num_rows()))

                # call join on the batch
                dmsg2 = utils.extend_inherit_message(dmsg, "__join__ batch: {}".format(i))
                tasks.append(utils.ThreadPoolTask(left_batches[i].__join__, right_batches[i], lkeys, rkeys, join_type = join_type, lsuffix = lsuffix, rsuffix = rsuffix,
                    default_val = default_val, def_val_map = def_val_map, num_par = 0, dmsg = dmsg2))

            # call thread executor
            results = utils.run_with_thread_pool(tasks, num_par = num_par)

            # merge
            return merge(results)

        # create a hashmap of left key values
        lvkeys = {}
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[1/3] building map for left side", dmsg, counter, self.num_rows())

            # parse data
            fields = line.split("\t")
            lvals1 = list([fields[i] for i in lkey_indexes])
            lvals2 = list([fields[i] for i in lvalue_indexes])
            lvals1_str = "\t".join(lvals1)

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
        for line in that.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[2/3] building map for right side", dmsg, counter, len(that.get_data()))

            # parse data
            fields = line.split("\t")
            rvals1 = list([fields[i] for i in rkey_indexes])
            rvals2 = list([fields[i] for i in rvalue_indexes])
            rvals1_str = "\t".join(rvals1)

            # right side values are not unique
            if (rvals1_str in rvkeys.keys()):
                utils.trace("right side values are not unique")

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
        new_header_copy_fields_map = {} 

        # create the keys
        for lkey in lkeys:
            new_header_fields.append(lkey)

        # print message for rkeys that are ignored
        for rkey_index in range(len(rkeys)):
            rkey = rkeys[rkey_index]
            if (rkey not in new_header_fields):
                utils.debug_once("rkey has a different name: {}".format(rkey))
                new_header_copy_fields_map[lkeys[rkey_index]] = rkey

        # add the left side columns
        for i in range(len(self.get_header_fields())):
            if (self.header_fields[i] not in lkeys):
                if (lsuffix is not None):
                    new_header_fields.append(self.header_fields[i] + ":" + lsuffix)
                else:
                    new_header_fields.append(self.header_fields[i])

        # add the right side columns
        for i in range(len(that.get_header_fields())):
            if (that.get_header_fields()[i] not in rkeys):
                if (rsuffix is not None):
                    new_header_fields.append(that.get_header_fields()[i] + ":" + rsuffix)
                else:
                    if (that.get_header_fields()[i] not in new_header_fields):
                        new_header_fields.append(that.get_header_fields()[i])
                    else:
                        raise Exception("Duplicate key names found. Use lsuffix or rsuffix: {}".format(that.get_header_fields()[i]))

        new_header_fields.append("__join_keys_matched__")
        # construct new_header
        new_header = "\t".join(new_header_fields)

        # define the default lvalues
        default_lvals = []
        for h in self.get_header_fields():
            if (h not in lkeys):
                if (def_val_map is not None and h in def_val_map.keys()):
                    default_lvals.append(def_val_map[h])
                else:
                    default_lvals.append(default_val)

        # define the default rvalues
        default_rvals = []
        for h in that.get_header_fields():
            if (h not in rkeys):
                if (def_val_map is not None and h in def_val_map.keys()):
                    default_rvals.append(def_val_map[h])
                else:
                    default_rvals.append(default_val)

        # generate output by doing join
        new_data = []

        # iterate over left side
        counter = 0
        for lvkey in lvkeys.keys():
            # report progress
            counter = counter + 1
            utils.report_progress("[3/3] join the two groups", dmsg, counter, len(self.get_data()))

            # get the values
            lvals2_arr = lvkeys[lvkey]
            rvals2_arr = [default_rvals]
            keys_matched = 0
            if (lvkey in rvkeys.keys()):
                  rvals2_arr = rvkeys[lvkey]
                  keys_matched = 1

            # do a MxN merge of left side values and right side values
            for lvals2 in lvals2_arr:
                for rvals2 in rvals2_arr:
                    # construct the new line
                    new_line = "\t".join(utils.merge_arrays([[lvkey], lvals2, rvals2, [str(keys_matched)]]))

                    # take care of different join types
                    if (join_type == "inner"):
                        if (lvkey in common_keys.keys()):
                            new_data.append(new_line)
                    elif (join_type == "left_outer" or join_type == "left"):
                            new_data.append(new_line)
                    elif (join_type == "right_outer" or join_type == "right"):
                        if (lvkey in common_keys.keys()):
                            new_data.append(new_line)
                    elif (join_type == "full_outer" or join_type == "outer"):
                        new_data.append(new_line)
                    else:
                        raise Exception("Unknown join type: {} ".format(join_type))

        # iterate over right side
        for rvkey in rvkeys.keys():
            # get the values
            rvals2_arr = rvkeys[rvkey]
            lvals2_arr = [default_lvals]
            if (rvkey in lvkeys.keys()):
                lvals2_arr = lvkeys[rvkey]

            # MxN loop for multiple rows on left and right side
            for lvals2 in lvals2_arr:
                for rvals2 in rvals2_arr:
                    # construct the new line
                    new_line = "\t".join(utils.merge_arrays([[rvkey], lvals2, rvals2, [str(keys_matched)]]))

                    # take care of different join types
                    if (join_type == "inner"):
                        pass
                    elif (join_type == "left_outer" or join_type == "left"):
                        pass
                    elif (join_type == "right_outer" or join_type == "right"):
                        if (rvkey not in common_keys.keys()):
                            new_data.append(new_line)
                    elif (join_type == "full_outer" or join_type == "outer"):
                        if (rvkey not in common_keys.keys()):
                            new_data.append(new_line)
                    else:
                        raise Exception("Unknown join type: {}".format(join_type))

        # return
        result = TSV(new_header, new_data)
        for lkey in new_header_copy_fields_map.keys():
            result = result.transform([lkey, "__join_keys_matched__"], lambda t1, t2: t1 if (t2 == "1") else "", new_header_copy_fields_map[lkey], dmsg = dmsg)
            
        # remove the temporary column
        result = result \
            .drop_cols("__join_keys_matched__", dmsg = dmsg)

        # return
        return result 

    # method to do map join. The right side is stored in a hashmap. only applicable to inner joins
    def natural_join(self, that, dmsg = ""):
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
            utils.report_progress("natural_join: [1/1] adding values from hashmap", dmsg, counter, self.num_rows())

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

    def inner_map_join(self, that, lkeys, rkeys = None, lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "inner_map_join")
        return self.__map_join__(that, lkeys, rkeys = rkeys, join_type = "inner", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, num_par = num_par, dmsg = dmsg)

    def left_map_join(self, that, lkeys, rkeys = None, join_type = "inner", lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "left_map_join")
        return self.__map_join__(that, lkeys, rkeys = rkeys, join_type = "left", lsuffix = lsuffix, rsuffix = rsuffix, default_val = default_val, def_val_map = def_val_map, num_par = num_par, dmsg = dmsg)

    def __map_join__(self, that, lkeys, rkeys = None, join_type = "inner", lsuffix = None, rsuffix = None, default_val = "", def_val_map = None, num_par = 0, dmsg = ""):
        # validation
        if (join_type not in ["inner", "left", "left_outer"]):
            raise Exception("__map_join__: join_type: {} is not supported".format(join_type))

        # matching
        lkeys = self.__get_matching_cols__(lkeys)
        rkeys = that.__get_matching_cols__(rkeys) if (rkeys is not None) else that.__get_matching_cols__(lkeys)

        # find the indexes
        lkey_indexes = []
        lvalue_indexes = []

        # relative ordering matters
        for h in lkeys:
            if (h in self.get_header_fields()):
                lkey_indexes.append(self.get_header_map()[h])

        for h in self.get_header_fields():
            if (h not in lkeys):
                lvalue_indexes.append(self.get_header_map()[h])

        rkey_indexes = []
        rvalue_indexes = []

        # relative ordering matters
        for h in rkeys:
            if (h in that.get_header_fields()):
                rkey_indexes.append(that.get_header_map()[h])

        for h in that.get_header_fields():
            if (h not in rkeys):
                rvalue_indexes.append(that.get_header_map()[h])

        # check the lengths
        if (len(lkeys) != len(rkeys)):
            raise Exception("Length mismatch in lkeys and rkeys: {}, {}".format(lkeys, rkeys))

        # print stats for left and right side
        utils.debug("__map_join__: left num_rows: {}, right num_rows: {}".format(self.num_rows(), that.num_rows()))

        # Check for num_par. TODO: Experimental
        if (num_par > 0):
            # split left and right sides
            left_batches = self.__split_batches_by_cols__(num_par, lkeys, seed = seed)
            right_batches = that.__split_batches_by_cols__(num_par, rkeys, seed = seed)

            # call join on individual batches and then return the merge
            tasks = []
            for i in range(num_par):
                # debug
                utils.debug("Calling join on batch: {}, left: {}, right: {}".format(i, left_batches[i].num_rows(), right_batches[i].num_rows()))

                # call join on the batch
                dmsg = utils.extend_inherit_message(dmsg, "__map_join__ batch: {}".format(i))
                tasks.append(utils.ThreadPoolTask(left_batches[i].__map_join__, right_batches[i], lkeys, rkeys, join_type = join_type, lsuffix = lsuffix, rsuffix = rsuffix,
                    default_val = default_val, def_val_map = def_val_map, num_par = 0, dmsg = dmsg))

            # call thread executor
            results = utils.run_with_thread_pool(tasks, num_par = num_par)

            # merge
            return merge(results)

        # create a hashmap of right key values
        rvkeys = {}
        counter = 0
        for line in that.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("__map_join__: building map for right side", dmsg, counter, len(that.get_data()))

            # parse data
            fields = line.split("\t")
            rvals1 = list([fields[i] for i in rkey_indexes]) 
            rvals2 = list([fields[i] for i in rvalue_indexes])

            # create value string for rkey
            rvals1_str = "\t".join(rvals1)

            # TODO: why this debug right side values are not unique
            # if (rvals1_str in rvkeys.keys()):
            #     utils.trace("right side values are not unique: rvals1: {}, rvals2: {}, rvkeys[rvals1_str]: {}".format(rvals1, rvals2, rvkeys[rvals1_str]))

            # check if the key already exists, else create an array
            if (rvals1_str not in rvkeys.keys()):
                rvkeys[rvals1_str] = []

            # append the value
            rvkeys[rvals1_str].append(rvals2)

        # for each type of join, merge the values
        new_header_fields = []
        new_header_copy_fields_map = {}

        # create the keys
        for lkey in lkeys:
            new_header_fields.append(lkey)

        # print message for rkeys that are ignored
        for rkey_index in range(len(rkeys)):
            rkey = rkeys[rkey_index]
            if (rkey not in new_header_fields):
                utils.debug_once("rkey has a different name: {}".format(rkey))
                new_header_copy_fields_map[lkeys[rkey_index]] = rkey

        # add the left side columns
        for i in range(len(self.get_header_fields())):
            if (self.header_fields[i] not in lkeys):
                if (lsuffix is not None):
                    new_header_fields.append(self.header_fields[i] + ":" + lsuffix)
                else:
                    new_header_fields.append(self.header_fields[i])

        # add the right side columns
        for i in range(len(that.get_header_fields())):
            if (that.get_header_fields()[i] not in rkeys):
                if (rsuffix is not None):
                    new_header_fields.append(that.get_header_fields()[i] + ":" + rsuffix)
                else:
                    if (that.get_header_fields()[i] not in new_header_fields):
                        new_header_fields.append(that.get_header_fields()[i])
                    else:
                        raise Exception("Duplicate key names found. Use lsuffix or rsuffix: {}".format(that.get_header_fields()[i]))

        # add a new column to depict if join leys matched
        new_header_fields.append("__map_join_keys_matched__")

        # construct new_header
        new_header = "\t".join(new_header_fields)

        # define the default rvalues
        default_rvals = []
        for h in that.get_header_fields():
            if (h not in rkeys):
                if (def_val_map is not None and h in def_val_map.keys()):
                    default_rvals.append(def_val_map[h])
                else:
                    default_rvals.append(default_val)

        # generate output by doing join
        new_data = []

        # iterate over left side
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("__map_join__: join the two groups", dmsg, counter, self.num_rows())

            # get fields
            fields = line.split("\t")

            # generate left side key and values
            lvals1 = list([fields[i] for i in lkey_indexes])
            lvals2 = list([fields[i] for i in lvalue_indexes])
            lvkey = "\t".join(lvals1)

            # get ride side values
            rvals2_arr = [default_rvals]
            keys_matched = 0
            if (lvkey in rvkeys.keys()):
                  rvals2_arr = rvkeys[lvkey]
                  keys_matched = 1

            # iterate on the right side
            for rvals2 in rvals2_arr:
                # construct the new line
                new_line = "\t".join(utils.merge_arrays([[lvkey], lvals2, rvals2, [str(keys_matched)]]))

                # take care of different join types
                if (join_type == "inner"):
                    if (lvkey in rvkeys):
                        new_data.append(new_line)
                elif (join_type == "left_outer" or join_type == "left"):
                    new_data.append(new_line)
                else:
                    raise Exception("Unknown join type: {} ".format(join_type))

        # create tsv
        result = TSV(new_header, new_data)

        # create the rkeys columns that had different names
        for lkey in new_header_copy_fields_map.keys():
            result = result.transform([lkey, "__map_join_keys_matched__"], lambda t1, t2: t1 if (t2 == "1") else "", new_header_copy_fields_map[lkey])

        # remove the temporary column
        result = result \
            .drop_cols("__map_join_keys_matched__", dmsg = dmsg)

        # return
        return result

    # public method handling both random and cols based splitting
    def split_batches(self, num_batches, cols = None, preserve_order = False, seed = None, dmsg = ""):
        # check for empty
        if (self.has_empty_header()):
            # check for cols
            if (cols is None):
                utils.warn("split_batches: empty tsv")
                return [self]
            else:
                raise Exception("split_batches: empty tsv")

        # check for empty rows
        if (self.num_rows() == 0):
            utils.warn("split_batches: empty tsv")
            return [self]

        # check if cols are defined or not
        dmsg = utils.extend_inherit_message(dmsg, "split_batches")
        if (cols is None):
            return self.__split_batches_randomly__(num_batches, preserve_order = preserve_order, seed = seed, dmsg = dmsg)
        else:
            return self.__split_batches_by_cols__(num_batches, cols, seed = seed, dmsg = dmsg)

    # split method to split randomly
    def __split_batches_randomly__(self, num_batches, preserve_order = False, seed = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "__split_batches_randomly__")

        # validation
        if (preserve_order == True ):
            if (seed is not None):
                raise Exception("__split_batches_randomly__: seed is only valid when preserve_order is False")
        else:
            if (seed is None):
                seed = 0

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
        counter = 0
        for i in range(len(self.get_data())):
            # report progress
            counter = counter + 1
            utils.report_progress("__split_batches_randomly__: [1/1] assigning batch index", dmsg, counter, self.num_rows())

            # check if original order of data needs to be preserved
            if (preserve_order == True):
                batch_index = int(i / batch_size)
            else:
                batch_index = (i + seed) % effective_batches

            # append to the splits data
            data_list[batch_index].append(self.data[i])

        # create list of xtsvs
        for i in range(effective_batches):
            xtsv_list.append(TSV(self.header, data_list[i]))

        # filter out empty batches
        xtsv_list = list(filter(lambda t: t.num_rows() > 0, xtsv_list))

        # return
        return xtsv_list

    # split method to split tsv into batches
    # TODO: add dmsg
    def __split_batches_by_cols__(self, num_batches, cols, seed = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "__split_batches_by_cols__")

        # get matching cols
        cols = self.__get_matching_cols__(cols)

        # create some temp column names
        temp_col1 = "__split_batches_by_cols__:hash:{}".format(num_batches)
        temp_col2 = "__split_batches_by_cols__:batch:{}".format(num_batches)

        # generate hash
        hashed_tsv = self.generate_key_hash(cols, temp_col1, seed = seed)

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
        counter = 0
        for line in hashed_tsv2.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("__split_batches_by_cols__: [1/1] assigning batch index", dmsg, counter, len(hashed_tsv2.get_data()))

            fields = line.split("\t")
            batch_id = int(fields[batch_index])
            new_data_list[batch_id].append(line)

        # create each tsv
        for i in range(len(new_data_list)):
            new_tsv = TSV(hashed_tsv2.get_header(), new_data_list[i]) \
                .drop_cols([temp_col1, temp_col2])
            new_tsvs.append(new_tsv)

        # filter out empty batches
        new_tsvs = list(filter(lambda t: t.num_rows() > 0, new_tsvs))

        # return
        return new_tsvs

    # method to generate a hash for a given set of columns
    def generate_key_hash(self, cols, new_col, seed = 0, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("generate_key_hash: empty tsv")

        # resolve cols
        cols = self.__get_matching_cols__(cols)
        indexes = self.__get_col_indexes__(cols)

        # validation
        if (new_col in self.get_columns()):
            raise Exception("new column already exists: {}".format(new_col))

        # generate deterministic hash
        new_header = "\t".join([self.header, new_col])
        new_data = []

        # iterate
        for line in self.get_data():
            fields = line.split("\t")
            values = []
            for i in indexes:
                values.append(str(fields[i]))

            # generate a single value
            value_str = "\t".join(values)

            # apply hash
            hash_value = str(utils.compute_hash(value_str, seed = seed))

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
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        # check for validity of new col
        if (new_col in self.header_map.keys()):
            raise Exception("New column already exists: {}, {}".format(str(new_col), str(self.get_header_fields())))

        # create new header
        new_header = self.header + "\t" + new_col

        # create new data
        new_data = []

        # cumulative sum
        cumsum = 0

        col_index = self.header_map[col]

        # iterate
        for line in self.get_data():
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
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        # create new column if it is not existing
        if (new_col is None):
            new_col = "{}:replicate_rows".format(col)

        # check new col
        if (new_col in self.header_map.keys()):
            raise Exception("New Column already exists: {}, {}".format(str(new_col), str(self.get_header_fields())))

        # create data
        new_data = []
        new_header = self.header + "\t" + new_col
        for line in self.get_data():
            fields = line.split("\t")
            col_value = int(fields[self.header_map[col]])
            # check for guard conditions
            if (max_repl > 0 and col_value > max_repl):
                raise Exception("repl_value more than max_repl: {}, {}".format(col_value, max_repl))

            # replicate
            for i in range(col_value):
                new_data.append(line + "\t" + "1")

        return TSV(new_header, new_data)

    # TODO: Need better naming. The suffix semantics have been changed.
    # default_val should be empty string
    # This doesnt handle empty data correctly. the output cols need add_empty_cols_if_missing
    def explode(self, cols, exp_func, prefix, default_val = None, collapse = True, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "explode")

        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("{}: empty tsv".format(dmsg), ignore_if_missing)
            return self

        # get matching column and indexes
        matching_cols = self.__get_matching_cols__(cols, ignore_if_missing = ignore_if_missing)
        indexes = self.__get_col_indexes__(matching_cols)

        # check for no matching cols
        if (len(matching_cols) == 0):
            utils.raise_exception_or_warn("{}: no matching cols: {}".format(dmsg, cols), ignore_if_missing)
            return self

        # iterate
        exploded_values = []
        counter = 0
        for line in self.get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("[1/2] calling explode function", dmsg, counter, self.num_rows())

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
                        utils.error_and_raise_exception("{}: Invalid key in the hashmap:{}: {}".format(dmsg, k, v))
                    exploded_keys[k] = 1

        # create an ordered list of keys
        exploded_keys_sorted = sorted(list(exploded_keys.keys()))

        # new header and data
        new_data = []

        # create header
        new_header_fields = []
        if (collapse == True):
            for j in range(len(self.get_header_fields())):
                if (j not in indexes):
                    new_header_fields.append(self.header_fields[j])
        else:
            # take care of not referencing self.header_fields
            for h in self.get_header_fields():
                new_header_fields.append(h)

        # create new names based on suffix
        exploded_keys_new_names = []

        # append new names to the exploded keys
        for e in exploded_keys_sorted:
            exploded_keys_new_names.append(prefix + ":" + e)

        # check if any of new keys clash with old columns
        for k in exploded_keys_new_names:
            if (k in self.get_header_fields()):
                raise Exception("Column already exist: {}, {}".format(k, str(self.get_header_fields())))

        # append to the new_header_fields
        for h in exploded_keys_new_names:
            new_header_fields.append(h)
        new_header = "\t".join(new_header_fields)

        # iterate and generate new data
        utils.print_code_todo_warning("explode: Verify this logic is not breaking anything. check TODO")

        counter = 0
        for i in range(len(self.get_data())):
            # report progress
            counter = counter + 1
            utils.report_progress("[2/2] generating data", dmsg, counter, self.num_rows())

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

            # check for each map in evm
            for evm in exploded_value_list_map:
                new_vals = []
                for k in exploded_keys_sorted:
                    # add to the output cols
                    if (k in evm.keys()):
                        new_vals.append(str(evm[k]))
                    else:
                        # put default value if defined
                        if (default_val is not None):
                            new_vals.append(str(default_val))
                        else:
                            utils.error_and_raise_exception("{}: Not all output values are returned from function and default_val is None: evm: {}, exploded_keys_sorted: {}, key: {}".format(
                                dmsg, evm, exploded_keys_sorted, k))

                # TODO: move this to a proper function
                new_data.append("\t".join(utils.merge_arrays([new_fields, new_vals])))

        # result. json expansion needs to be validated because of potential noise in the data.
        return TSV(new_header, new_data) \
            .validate()

    def __explode_json_transform_func__(self, col, accepted_cols, excluded_cols, single_value_list_cols, transpose_col_groups, merge_list_method, url_encoded_cols,
        nested_cols, collapse_primitive_list, max_results = None, join_col = ","):

        # constant
        json_explode_index = "__explode_json_index__"

        # inner function that is returned
        def __explode_json_transform_func_inner__(mp):
            # some validation.
            if (col not in mp.keys() or mp[col] == "" or mp[col] is None):
                utils.trace_once("__explode_json_transform_func_inner__: potentially invalid json response found. Usually it is okay. But better to check: {}, {}".format(col, mp))
                mp = {"__explode_json_len__": "0"}
                return [mp]

            # fetch the json string, it might be url encoded or not though recommendation is to use url encoding.
            json_str = mp[col]
            json_mp = None

            # best effort in detecting the type and parsing for robustness
            if (json_str.startswith("%7B") == False and json_str.startswith("%5B") == False):
                if (json_str.startswith("{") or json_str.startswith("[")):
                    utils.warn_once("explode_json called with column that is not url encoded json. Assuming plain json string")
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

            # Check if the number of results need to be capped
            if (max_results is not None and len(results) > max_results):
                utils.warn("__explode_json_transform_func__: capping the number of results from {} to {}".format(len(results), max_results))
                results = results[0:max_results]

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
                    utils.warn_once("__explode_json_transform_func_expand_json__: None type value found. Taking it as empty string. Key: {}".format(k))
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
                            utils.warn_once("explode_json: multiple lists are not fully supported. Confirm data parsing or Use accepted_cols or excluded_cols: {}".format(str(k)))

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
                            raise Exception("Unknown data type: {}".format(type(v[0])))
                    elif (isinstance(v, dict) and len(v) > 0):
                        # warn for non trivial case
                        if (len(dict_results) > 0 and utils.is_debug()):
                            utils.warn_once("explode_json: multiple maps are not fully supported. Confirm data parsing or Use accepted_cols or excluded_cols: {}".format(str(k)))

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
                        raise Exception("WIP API. Dont use prefix name with ':' though the colon will be used as separator: {}".format(transpose_col_group_prefix))

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
                raise Exception("Unknown merge_list_method: {}".format(merge_list_method))

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

            # trace
            if (len(results) >= 100):
                utils.trace("__explode_json_transform_func_expand_json__: with count: {} >= 10, parent_prefix: {}, results[0]: {}".format(len(results), parent_prefix, results[0]))

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
        merge_list_method = "cogroup", collapse_primitive_list = True, url_encoded_cols = None, nested_cols = None, collapse = True, max_results = None, ignore_if_missing = False,
        default_val = "", dmsg = ""):

        # validation
        if (prefix is None):
            utils.warn("explode_json: prefix = None is deprecated. Using the original column name as prefix: {}".format(col))
            prefix = col

        # check empty
        if (self.has_empty_header()):
            utils.raise_exception_or_warn("explode_json: empty tsv", ignore_if_missing)
            return self

        # warn
        if (excluded_cols is not None):
            utils.print_code_todo_warning("explode_json: excluded_cols is work in progress and may not work in all scenarios")

        # validation
        if (col not in self.header_map.keys()):
            utils.raise_exception_or_warn("Column not found: {}, {}".format(str(col), str(self.get_header_fields())), ignore_if_missing)
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
            transpose_col_groups = transpose_col_groups, merge_list_method = merge_list_method, url_encoded_cols = url_encoded_cols, nested_cols = nested_cols,
            collapse_primitive_list = collapse_primitive_list, max_results = max_results)

        # use explode to do this parsing
        dmsg = utils.extend_inherit_message(dmsg, "explode_json")
        return self \
            .add_seq_num(prefix + ":__json_index__", dmsg = dmsg) \
            .explode([col], exp_func, prefix = prefix, default_val = default_val, collapse = collapse, dmsg = dmsg) \
            .validate()

    def transpose(self, n = 1, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "transpose")
        return self.take(n).__transpose_topn__(dmsg = dmsg)

    def __transpose_topn__(self, dmsg = ""):
        # construct new header
        new_header_fields = []
        new_header_fields.append("col_name")
        for i in range(self.num_rows()):
            new_header_fields.append("row:" + str(i + 1))
        new_header = "\t".join(new_header_fields)

        # create col arrays and new_data
        new_data = []
        for h in self.get_header_fields():
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
            raise Exception("Column not found: {}, {}".format(str(col), str(self.get_header_fields())))

        # check for new column
        if (new_col in self.header_map.keys()):
            raise Exception("New Column already exists: {}, {}".format(str(new_col), str(self.get_header_fields())))

        # create new data
        new_data = []
        new_header = self.header + "\t" + new_col

        # iterate
        for line in self.get_data():
            fields = line.split("\t")
            col_value = fields[self.header_map[col]]
            new_vals = func(col_value)
            for new_val in new_vals:
                new_data.append(line + "\t" + new_val)

        return TSV(new_header, new_data)

    def to_tuples(self, cols, dmsg = ""):
        # check empty
        if (self.has_empty_header()):
            raise Exception("to_tuples: empty tsv")

        # validate cols
        for col in cols:
            if (self.has_col(col) == False):
                raise Exception("col doesnt exist: {}, {}".format(col, str(self.get_header_fields())))

        # select the cols
        result = []

        # progress counters
        counter = 0
        dmsg = utils.extend_inherit_message(dmsg, "to_tuples")
        for line in self.select(cols, dmsg = dmsg).get_data():
            # report progress
            counter = counter + 1
            utils.report_progress("to_tuples: [1/1] converting to tuples", dmsg, counter, len(self.get_data()))

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
            raise Exception("Length of values is more than 10. Not supported: {}".format(str(vs)))

    # this method sets the missing values for columns
    def set_missing_values(self, cols, default_val, ignore_if_missing = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "set_missing_values")
        return self.transform_inline(cols, lambda x: x if (x != "") else default_val, ignore_if_missing = ignore_if_missing, dmsg = dmsg)

    # calls class that inherits TSV
    def extend_class(self, newclass, *args, **kwargs):
        return newclass(self.header, self.data, *args, **kwargs)

    # calls class that inherits TSV. TODO: forgot the purpose
    def extend_external_class(self, newclass, *args, **kwargs):
        utils.warn("extend_external_class: not sure the purpose of this method now. use extend_class instead")
        return newclass(self.header, self.data, *args, **kwargs)

    # custom function to call user defined apis
    def custom_func(self, func, *args, **kwargs):
        # boundary condition
        if (self.is_empty()):
            utils.warn_once("custom_func: header is empty. Make sure the custom_func handles empty data scenario and change this")

        # there can be scenarios of adding new columns
        if (self.num_rows() == 0):
            utils.warn_once("custom_func: input is empty. Make sure the custom_func handles empty data scenario and change this")

        # call function and return
        return func(self, *args, **kwargs)

    # taking the clipboard functionality from pandas
    def to_clipboard(self):
        self.to_df().to_clipboard()
        return self

    def __convert_to_maps__(self):
        result = []
        for line in self.get_data():
            mp = {}
            fields = line.split("\t")
            for h in self.header_map.keys():
                mp[h] = fields[self.header_map[h]]

            result.append(mp)

        return result

    # placeholder for new method to do xpath based filtering for json blobs
    def filter_json_by_xpath(self, col, xpath_filter, dmsg = ""):
        raise Exception("Not implemented yet")

    def get_col_index(self, col):
        utils.warn("Use get_column_index and remove this function")

        # check empty
        if (self.has_empty_header()):
            raise Exception("get_col_index: empty tsv")

        # validation
        if (col not in self.get_columns()):
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
        for line in self.get_data():
            hashes.append("{}".format(utils.compute_hash(line)))

        # return as string
        return "{}".format(utils.compute_hash(",".join(hashes)))

    # TODO: confusing name. is_empty can also imply no data
    def is_empty(self):
        return self.get_header() == ""

    def has_empty_header(self):
        return self.num_cols() == 0

    def write(self, path):
        tsvutils.save_to_file(self, path)
        utils.debug("write: {}".format(path))
        return self

    def show_custom_func(self, n, title, func, *args, **kwargs):
        # call show transpose after custom func
        self \
            .custom_func(func, *args, **kwargs) \
            .show(n = n, title = title)

        # return self
        return self

    def show_group_count(self, col_or_cols, n = 20, title = "Group Count", max_col_width = 40, sort_by_key = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "show_group_count")

        # call show transpose after custom func
        result = self \
            .group_count(col_or_cols, dmsg = dmsg)

        # sorting
        if (sort_by_key == True):
            result = result \
                .sort(col_or_cols)

        # show
        result \
            .show(n = n, title = title, max_col_width = max_col_width, dmsg = dmsg)

        # return self
        return self

    def show_select_func(self, col_or_cols, n = 20, title = "Group Count", max_col_width = 40, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "show_select_func")

        # show
        self \
            .select(col_or_cols, dmsg = dmsg) \
            .show(n = n, title = title, max_col_width = max_col_width, dmsg = dmsg)

        # return self
        return self

    def show_transpose_custom_func(self, n, title, func, *args, **kwargs):
        # call show transpose after custom func
        self \
            .custom_func(func, *args, **kwargs) \
            .show_transpose(n = n, title = title)

        # return self
        return self

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

        # check for wild card
        if (col_or_cols == ".*" or col_or_cols[0] == ".*"):
            return self.get_columns()

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

        # transform col_patterns into a stronger prefix or suffix match wherever applicable
        col_patterns_transformed = []
        for col_pattern in col_patterns:
            num_wild_cards = len(col_pattern.split("*")) - 1 if (col_pattern.find(".*") != -1) else 0

            # check how many wild cards are present
            if (num_wild_cards > 0):
                # check for prefix and suffix
                if (num_wild_cards == 1):
                    # check for prefix
                    if (col_pattern.startswith(".*") == True):
                        if (col_pattern != ".*" and col_pattern.endswith("$") == False):
                            utils.debug_once("__get_matching_cols__: rewriting pattern: {}".format(col_pattern))
                            col_pattern = str(col_pattern) + "$"

                    # check for suffix
                    if (col_pattern.endswith(".*") == True):
                        if (col_pattern != ".*" and col_pattern.startswith("^") == False):
                            utils.debug_once("__get_matching_cols__: rewriting pattern: {}".format(col_pattern))
                            col_pattern = "^" + str(col_pattern)
                elif (num_wild_cards == 2):
                    # check if wild cards are anywhere except as prefix or suffix
                    if (col_pattern.startswith(".*") == False or col_pattern.endswith(".*") == False):
                        utils.warn_once("__get_matching_cols__: multiple wildcards in col patterns can be confusing: {}".format(col_pattern))
                else:
                    utils.warn_once("__get_matching_cols__: multiple wildcards in col patterns can be confusing: {}".format(col_pattern))

            # append
            col_patterns_transformed.append(col_pattern)

        # now iterate through all the column names, check if it is a regular expression and find
        # all matching ones
        matching_cols = []
        for col_pattern in col_patterns_transformed:
            # check for matching columns for the pattern
            col_pattern_found = False

            # iterate through header
            for h in self.get_header_fields():
                # check for match
                if ((col_pattern.find(".*") != -1 and re.match(col_pattern, h) is not None) or (col_pattern == h)):
                    col_pattern_found = True
                    # if not in existing list then add
                    if (h not in matching_cols):
                        matching_cols.append(h)

            # raise exception if some col or pattern is not found
            if (col_pattern_found == False):
                utils.raise_exception_or_warn("Col name or pattern not found: {}, {}".format(col_pattern, str(self.get_header_fields())), ignore_if_missing)
                # dont return from here 

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
    def print_stats(self, msg = None):
        prefix = msg + ": " if (msg is not None) else ""

        # get display size
        bsize = self.size_in_bytes()
        size_str = ""
        if (bsize > 1e9):
            size_str = "{} GB".format(self.size_in_gb())
        elif (bsize > 1e6):
            size_str = "{} MB".format(self.size_in_mb())
        else:
            size_str = "{} bytes".format(bsize)

        msg2 = "{}num_rows: {}, num_cols: {}, size: {}".format(prefix, self.num_rows(), self.num_cols(), size_str)
        utils.info(msg2)
        return self

    # get top k columns by byte size
    def get_max_size_cols_stats(self, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "get_max_size_cols_stats")

        # create array to hold sizes
        total_sizes = list([0 for c in self.get_columns()])
 
        # data validation
        counter = 0
        for line in self.get_data():
            counter = counter + 1
            utils.report_progress("[1/1] selecting columns", dmsg, counter, self.num_rows())
            fields = line.split("\t")
            for i in range(len(fields)):
                total_sizes[i] = total_sizes[i] + len(fields[i])

        # find the max size
        max_value = max(total_sizes)

        # find the columns with the max size
        max_value_indexes = list(filter(lambda i: total_sizes[i] == max_value, range(len(total_sizes))))
        
        # create map with the list of indexes and their size, relative size
        mps = []
        for i in max_value_indexes:
            mp = {}
            mp["col"] = self.get_columns()[i]
            mp["size_in_bytes"] = max_value
            mp["relative_size"] = "{0:.4f}".format(max_value / sum(total_sizes))
            mps.append(mp)

        # return
        return mps
                
    # some methods for effects
    def sleep(self, secs):
        time.sleep(secs)
        return self

    def __split_exp_func__(self, cols, sep):
        def __split_exp_func_inner__(mp):
            result_mps = []
            values_arr = []
            for c in cols:
                values_arr.append(mp[c].split(sep))

            # do validation
            num_values = len(values_arr[0])
            for i in range(len(values_arr) - 1):
                if (len(values_arr[i+1]) != num_values):
                    raise Exception("__split_exp_func__: number of unique values are not same: {}".format(mp))

            # iterate and generate result maps
            for i in range(num_values):
                result_mp = {}
                for j in range(len(cols)):
                    result_mp[str(cols[j])] = str(values_arr[j][i])

                # append
                result_mps.append(result_mp)

            # return
            return result_mps

        # return
        return __split_exp_func_inner__

 
    def split(self, *args, **kwargs):
        utils.warn_once("split: use split_str method instead")
        return self.split_str(*args, **kwargs)
 
    def split_str(self, col_or_cols, prefix, sep = ",", collapse = True, dmsg = ""):
        # resolve columns
        cols = self.__get_matching_cols__(col_or_cols)
        new_cols = list(["{}:{}".format(prefix, t) for t in cols])

        # call explode
        dmsg = "{}: split".format(dmsg) if (len(dmsg) > 0) else "split"
        return self \
            .explode(col_or_cols, self.__split_exp_func__(cols, sep), prefix, collapse = collapse, dmsg = dmsg) \
            .add_empty_cols_if_missing(new_cols)

    def sample_group_by_topk(self, grouping_cols, sort_col, k, use_string_datatype = False, reverse = False, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "sample_group_by_topk")
        utils.warn_once("sample_group_by_topk: this needs to do deterministic selection when there are more entries of same sort_col value")

        def __sample_group_by_topk_inner_func__(mps):
            sorted_mps = sorted(mps, key = lambda t: str(t[sort_col]) if (use_string_datatype == True) else float(t[sort_col]))
            sorted_mps = sorted_mps[0:k] if (reverse == False) else sorted_mps[-k:]
            result = {}
            result["__top_selected_indexes__"] = ",".join([mp["__sample_group_by_topk_sno__"] for mp in sorted_mps])
            return result

        # return
        return self \
            .add_seq_num("__sample_group_by_topk_sno__", dmsg = dmsg) \
            .group_by_key(grouping_cols, [sort_col, "__sample_group_by_topk_sno__"], __sample_group_by_topk_inner_func__, suffix = "__sample_group_by_topk_inner_func__", collapse = False, dmsg = dmsg) \
            .filter(["__sample_group_by_topk_sno__", "__top_selected_indexes__:__sample_group_by_topk_inner_func__"], lambda t1, t2: t1 in t2.split(","), dmsg = dmsg) \
            .drop_cols(["__sample_group_by_topk_sno__", "__top_selected_indexes__:__sample_group_by_topk_inner_func__"], dmsg = dmsg)

    # this method fills values of this type {col} in the template col with the value of the col. If col_or_cols is None, then all remaining columns are used
    # this is an expensive query as it has to lookup each possible input col
    def resolve_template_col(self, template_col, output_col, col_or_cols = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "resolve_template_col")

        # validation
        if (template_col not in self.get_columns()):
            raise Exception("template_col: {} is not present".format(template_col))

        # validation
        if (output_col in self.get_columns()):
            raise Exception("output_col: {} already exists ".format(output_col))

        # some guards
        if (col_or_cols is None and self.num_rows() >= 100):
            utils.warn("{}: col_or_cols is None and the data hqs more than 100 rows. This api is expensive and defining col_or_cols can speed up".format(dmsg))

        # validation
        input_cols = self.__get_matching_cols__(col_or_cols) if (col_or_cols is not None) else list(filter(lambda t: t != template_col, self.get_columns()))

        # validation
        if (template_col in input_cols):
            raise Exception("template_col: {} is also in input columns: {}".format(template_col, input_cols))

        # explode function
        def __resolve_template_col_transform_func__(mp):
            template_value = mp[template_col]

            # iterate
            for input_col in input_cols:
                input_val = str(mp[input_col])
                input_template = "{" + input_col + "}"

                # check if the input col is present in template form
                if (template_value.find(input_template) != -1):
                    template_value = template_value.replace(input_template, input_val)

            # generate output
            result_mp = {output_col: template_value}

            # return
            return [result_mp]


        # input for explode
        explode_cols = input_cols + [template_col]

        # return
        return self \
            .explode(explode_cols, __resolve_template_col_transform_func__, "__resolve_template_col__", collapse = False, dmsg = dmsg) \
            .remove_prefix("__resolve_template_col__", dmsg = dmsg)

    def resolve_template_col_inline(self, template_col, col_or_cols = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "resolve_template_col_inline")

        # temp output col
        output_col = "__resolve_template_col_inline_output__"

        # return
        return self \
            .resolve_template_col(template_col, output_col, col_or_cols = col_or_cols, dmsg = dmsg) \
            .drop_cols(template_col, dmsg = dmsg) \
            .rename(output_col, template_col, dmsg = dmsg)
 
    def enable_debug_mode(self):
        utils.enable_debug_mode()
        return self

    def disable_debug_mode(self):
        utils.disable_debug_mode()
        return self

    def enable_info_mode(self):
        utils.enable_info_mode()
        return self

    def disable_info_mode(self):
        utils.disable_info_mode()
        return self

def get_version():
    return "v0.3.9:empty_tsv"

def get_func_name(f):
    return f.__name__

def read(paths, sep = None, do_union = False, def_val_map = None):
    # TODO: remove this after fixing design
    if (def_val_map is not None and do_union == False):
        raise Exception("Use do_union flag instead of relying on def_val_map to be non None")

    # check if union needs to be done. default is intersect
    if (do_union == False):
        return tsvutils.read(paths, sep = sep)
    else:
        # check if default values are checked explicitly
        if (def_val_map is None):
            def_val_map = {}

        return tsvutils.read(paths, sep = sep, def_val_map = {})

def write(xtsv, path):
    return xtsv.write(path)

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

def __is_builtin_func__(func):
    # check type
    if (type(func) in (type(sum), type(math.ceil))):
        return True
    else:
        return False

