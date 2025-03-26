# from IPython.display import IFrame
# from bokeh.io import curdoc
from IPython.display import display
from bokeh.embed import json_item
from bokeh.io import output_notebook, show
from bokeh.io import output_notebook, show
from bokeh.io import output_notebook, show, output_file, curdoc
from bokeh.layouts import row, column, Row, Column
from bokeh.models import ColumnDataSource, DataTable, DateFormatter, TableColumn
from bokeh.models import ColumnDataSource, DataTable, DateFormatter, TableColumn
from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar, NumeralTickFormatter, MultiLine, Div
from bokeh.models import GraphRenderer, Ellipse, StaticLayoutProvider, Circle
from bokeh.models import HoverTool, value, LabelSet, Legend, ColumnDataSource,LinearColorMapper,BasicTicker, PrintfTickFormatter, ColorBar
from bokeh.palettes import *
from bokeh.palettes import Category20c
from bokeh.palettes import Spectral8
from bokeh.plotting import figure, ColumnDataSource
from bokeh.plotting import figure, show
from bokeh.plotting import show, figure
from bokeh.tile_providers import get_provider, Vendors
from bokeh.transform import cumsum
from bokeh.transform import linear_cmap,factor_cmap
from datetime import date
from math import pi
from msticpy.nbtools import process_tree as ptree
from msticpy.sectools import proc_tree_schema
from omigo_core import tsv, utils, funclib, tsvutils, etl
from omigo_ext import splunk_ext
from omigo_hydra import cluster_common_v2
from omigo_proxy import ip2location_ext
from random import randint
import logging
import math
import numpy as np
import pandas as pd
import random
import time
from bokeh.plotting import output_notebook, show, figure, reset_output, output_file

def get_cluster_handler():
    return cluster_common_v2.ClusterPaths.get_cluster_handler()

# import os
# os.environ["BOKEH_ALLOW_WS_ORIGIN"] = "localhost:8889"

# Base class for dynamic graph layout
class GraphLayout:
    def __init__(self, xgraph_refs, width = 1700, height = 1000, refresh_sec = 10, output_path = None):
        self.xgraph_refs = xgraph_refs
        self.width = width
        self.height = height
        self.refresh_sec = refresh_sec
        self.output_path = output_path

        # initialize figure
        self.xgraphs = self.__flatten_graphs__(xgraph_refs)
        self.fig = None

        # initialize output path
        if (self.output_path is not None):
            get_cluster_handler().create(self.output_path)
            get_cluster_handler().create(self.output_path + "-chat")

    def init_source_figure(self):
        # initialize source and figure
        for xgraph in self.xgraphs:
            xgraph.init_source_figure()

        # create cols
        xfigs_cols = []

        # initialize per graph height
        xheight = int(self.height / len(self.xgraph_refs))

        # iterate
        for xgraph_ref in self.xgraph_refs:
            if (isinstance(xgraph_ref, list)):
                xfigs = []
                # store in a proper variable
                xgraphs_inner = xgraph_ref
                xwidth = int(self.width / len(xgraphs_inner))

                # adjust the width of the figure
                for xgraph_inner in xgraphs_inner:
                    xgraph_inner.set_width_height(xwidth, xheight)
                    xfigs.append(xgraph_inner.get_figure())

                # add row
                xfigs_cols.append(row(xfigs))
            else:
                xgraph_inner = xgraph_ref
                xgraph_inner.set_width_height(self.width, xheight)
                xfigs_cols.append(xgraph_inner.get_figure())

        # create a column layout
        self.fig = column(xfigs_cols)

        # persist
        # if (self.output_path is not None):
        #    get_cluster_handler().update_json("{}/{}".format(self.output_path, cluster_common_v2.construct_dynamic_value_json()), json_item(self.fig))


    def __flatten_graphs__(self, xgraph_refs):
        xgraphs = []
        for xgraph_ref in xgraph_refs:
            # check if a list or single
            if (isinstance(xgraph_ref, list)):
                for xgraph in self.__flatten_graphs__(xgraph_ref):
                    xgraphs.append(xgraph)
            elif (isinstance(xgraph_ref, (Row, Column))):
                for xgraph in self.__flatten_graphs__(list(xgraph_ref.children)):
                    xgraphs.append(xgraph)
            else:
                xgraphs.append(xgraph_ref)

        # return
        return xgraphs

    def plot(self):
        def __demo_plot_inner__(doc):
            # iterate through each of the xgraph and call the plot function
            for xgraph in self.xgraphs:
                # plot_func = xgraph.get_plot_inner_func()
                # plot_func(doc)
                xgraph.init_plot()

            def __demo_plot_inner_update__():
                # iterate through all xgraphs
                for xgraph in self.xgraphs:
                    plot_update_func = xgraph.get_plot_inner_update_func()
                    plot_update_func()

                # persist
                if (self.output_path is not None):
                    get_cluster_handler().update_json("{}/{}".format(self.output_path, cluster_common_v2.construct_dynamic_value_json()), json_item(self.fig))

            # add periodic update
            doc.add_periodic_callback(__plot_inner_update__, self.refresh_sec * 1000)

            # add root
            doc.add_root(self.fig)

        # main function
        self.init_source_figure()
        output_notebook()
        show(__demo_plot_inner__)


# Base class for each dynamic visualization
class BaseGraph:
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        figure_name = None, delta_update_flag = None, output_path = None):

        self.input_path_map = input_path_map
        self.interval = interval
        self.window = window
        self.cols = cols
        self.start_ts_col = start_ts_col
        self.end_ts_col = end_ts_col
        self.rollover = rollover
        self.width = width
        self.height = height
        self.title = title
        self.refresh_sec = refresh_sec
        self.start_date_str = start_date_str
        self.end_date_str = end_date_str
        self.filter_transform_func = filter_transform_func
        self.delta_update_flag = delta_update_flag
        self.output_path = output_path

        # initialize start and end date string correctly
        self.start_date_str, self.end_date_str = self.__infer_start_and_end_dates__(start_date_str, end_date_str)

        # some variables that are initialized later
        self.sources = None
        self.fig = None
        self.local_xtsv_base = None

    # TODO: drop the seconds
    def __infer_start_and_end_dates__(self, sdate_str, edate_str):
        if (sdate_str is None and edate_str is None):
            edate_str = funclib.utctimestamp_to_datetime_str(funclib.get_utctimestamp_sec())
            sdate_str = funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(edate_str) - self.interval * self.rollover)
        elif (sdate_str is None and edate_str is not None):
            sdate_str = funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(edate_str) - self.interval * self.rollover)
        elif (sdate_str is not None and edate_str is None):
            edate_str = funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(sdate_str) + self.interval * self.rollover)

        # normalize date strings
        sdate_str = funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(sdate_str))
        edate_str = funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(edate_str))

        utils.info("__infer_start_and_end_dates__: start: {}, end: {}".format(sdate_str, edate_str))
        return sdate_str, edate_str

    def get_next_data_update(self, local_xtsv_base):
        raise Exception("Derived class should implement")

    def refresh_plot_data(self, xtsv):
        raise Exception("Derived class should implement")

    def create_sources(self):
        raise Exception("Derived class should implement")

    def create_figure(self):
        raise Exception("Derived class should implement")

    def init_source_figure(self):
        # create source and figure. the figure needs the source so can only be done after source is created
        self.sources = self.create_sources()
        self.fig = self.create_figure()

    def init_plot(self):
        # timestamps
        self.local_end_ts = funclib.datetime_to_utctimestamp(self.start_date_str)
        self.local_finished_ts = funclib.datetime_to_utctimestamp(self.end_date_str)

        # read data slice
        self.local_xtsv_base = tsv.new_with_cols(self.cols)

    def set_width_height(self, xwidth, xheight):
        if (isinstance(self.fig, Row)):
            for xchild in self.fig.children:
                if (isinstance(xchild, Div)):
                    xchild.height = xheight
                else:
                    xchild.width = xwidth
                    xchild.height = xheight
        elif (isinstance(self.fig, Column)):
            for xchild in self.fig.children:
                if (isinstance(xchild, Div)):
                    xchild.width = xwidth
                else:
                    xchild.width = xwidth
                    xchild.height = xheight
        else:
            self.fig.width = xwidth
            self.fig.height = xheight

    def get_plot_inner_func(self):
        def __demo_plot_inner__(doc):
            # initialize plot
            self.init_plot()

            # show(data_table)
            print("calling add_root")
            doc.add_periodic_callback(self.get_plot_inner_update_func(), self.refresh_sec * 1000)
            doc.add_root(self.fig)

        return __plot_inner__

    def get_plot_inner_update_func(self):
        # periodic callback function
        def __demo_plot_inner_update__():
            # check if local_end_ts is more that current timestamp
            if (self.local_end_ts > self.local_finished_ts - self.window):
                utils.info("local_end_ts:{} > local_finished_ts:{} - self.window:{}".format(self.local_end_ts, self.local_finished_ts, self.window))
                # ignore
                return

            # get new data
            xtsv_map, self.local_xtsv_base = self.get_next_data_update(self.local_end_ts, self.local_xtsv_base)

            # update local_end_ts
            self.local_end_ts = self.local_end_ts + self.interval

            # debug
            if (xtsv.num_rows() > 0):
                utils.info("__plot_inner_update__ {}: {}, num_rows: {}".format(self.input_path, funclib.utctimestamp_to_datetime_str(self.local_end_ts), xtsv.num_rows()))

            # call delta update or replace
            self.refresh_plot_data(xtsv)

        return __plot_inner_update__

    def plot(self):
        # initialize
        self.init_source_figure()
        self.init_plot()

        # main function
        output_notebook()
        show(self.get_plot_inner_func())

    def get_figure(self):
        return self.fig

    def get_output_path(self):
        return self.output_path

# Static process tree
class StaticGraphHostProcessTree(BaseGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path)

    # create source
    def create_sources(self):
        # create ColumnDataSource, and define table columns
        mp = {}
        for c in self.cols:
            mp[c] = []

        # create source
        return {"default": ColumnDataSource(mp)}

    def get_next_data_update(self, cur_end_ts, local_xtsv_base):
        # return
        return local_xtsv_base, local_xtsv_base

    def refresh_plot_data(self, xtsv):
        # Do nothing
        pass

    def create_figure(self):
        # construct start and end dates
        sdate_str = self.start_date_str
        edate_str = self.end_date_str

        # get the timeestamps
        sdate_ts = funclib.datetime_to_utctimestamp(sdate_str)
        edate_ts = funclib.datetime_to_utctimestamp(edate_str)

        # read base input
        xtsv = etl.scan_by_datetime_range(self.input_path_map["default"], sdate_str, edate_str, "output", num_par = 0, def_val_map = {})

        # check filter function on xtsv
        if (self.filter_transform_func is not None):
            xtsv = self.filter_transform_func(xtsv)

        # for each missing col, add to the data
        xtsv = xtsv.add_empty_cols_if_missing(self.cols)

        # select the output cols
        xtsv = xtsv.select(self.cols)

        # apply the timestamp col filter. Call datetime conversion as the ts_col can be string or numeric format
        if (self.end_ts_col is not None):
            xtsv = xtsv \
                .not_eq_str(self.end_ts_col, "") \
                .filter(self.end_ts_col, lambda t: funclib.datetime_to_utctimestamp(t) >= sdate_ts and funclib.datetime_to_utctimestamp(t) < edate_ts) \
                .sort(self.end_ts_col)

        # These constants are part of ms test data and their bokeh visualization: https://msticpy.readthedocs.io/en/latest/visualization/ProcessTree.html
        # The actual names should come from their proc_tree_schema
        xtsv_dict = proc_tree_schema.WIN_EVENT_SCH
        xtsv_dict.time_stamp = "proxy_time_stamp"
        xtsv_dict.process_name = "proxy_process_name"
        xtsv_dict.process_id = "proxy_process_id"
        xtsv_dict.parent_name = "proxy_parent_name"
        xtsv_dict.parent_id = "proxy_parent_id"
        xtsv_dict.logon_id = "proxy_logon_id"
        xtsv_dict.target_logon_id = "proxy_target_logon_id"
        xtsv_dict.cmd_line = "proxy_cmd_line"
        xtsv_dict.user_name = "proxy_user_name"
        xtsv_dict.path_separator="\\"
        xtsv_dict.user_id = "proxy_user_id"
        xtsv_dict.event_id_column = "proxy_event_id"
        xtsv_dict.event_id_identifier = 4688
        xtsv_dict.host_name_column = "proxy_host"

        df = xtsv.to_df(infer_data_types = True)
        p_tree_win = ptree.build_process_tree(df, schema = xtsv_dict, show_summary=False)

        label = Div(text = "<h3>{}</h3>".format(self.title))
        fig = ptree.plot_process_tree_omigo(data = p_tree_win, legend_col = "UserSid", show_table = True, height = 400, pid_fmt = "dec", title = None)
        return column(label, fig)

    def set_width_height(self, xwidth, xheight):
        pass

class StaticGraphHorzLine(BaseGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path)

    # create source
    def create_sources(self):
        # create ColumnDataSource, and define table columns
        mp = {}
        for c in self.cols:
            mp[c] = []

        # create source
        return {"default": ColumnDataSource(mp)}

    def get_next_data_update(self, cur_end_ts, local_xtsv_base):
        # return
        return local_xtsv_base, local_xtsv_base

    def refresh_plot_data(self, xtsv):
        # Do nothing
        pass

    def create_figure(self):
        hr = Div(text = """<hr size="10">""", width = self.width, height = self.height)
        return hr

    def set_width_height(self, xwidth, xheight):
        self.fig.width = xwidth

class ContinuousGraph(BaseGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None, include_end_boundary = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path)
        self.include_end_boundary = include_end_boundary

    # create source
    def create_sources(self):
        # create ColumnDataSource, and define table columns
        mp = {}
        for c in self.cols:
            mp[c] = []

        # create source
        return {"default": ColumnDataSource(mp)}

    def get_next_data_update(self, cur_end_ts, local_xtsv_base):
        # construct start and end dates
        sdate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts - self.interval - self.window)
        edate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts)

        # get the timeestamps
        sdate_ts = funclib.datetime_to_utctimestamp(sdate_str)
        edate_ts = funclib.datetime_to_utctimestamp(edate_str)

        # read base input
        xtsv = etl.scan_by_datetime_range(self.input_path_map["default"], sdate_str, edate_str, "output", num_par = 0, def_val_map = {})

        # check filter function on xtsv
        if (self.filter_transform_func is not None):
            xtsv = self.filter_transform_func(xtsv)

        # for each missing col, add to the data
        xtsv = xtsv.add_empty_cols_if_missing(self.cols)
        xtsv.add_empty_cols_if_missing([self.start_ts_col])
        xtsv.add_empty_cols_if_missing([self.end_ts_col])

        # check for empty
        if (xtsv.num_rows() == 0):
            return xtsv, local_xtsv_base

        def __get_next_data_update_end_boundary_inner__(t):
            if (self.include_end_boundary == True):
                return funclib.datetime_to_utctimestamp(t) >= sdate_ts and funclib.datetime_to_utctimestamp(t) <= edate_ts
            else:
                return funclib.datetime_to_utctimestamp(t) >= sdate_ts and funclib.datetime_to_utctimestamp(t) < edate_ts

        # apply the timestamp col filter. Call datetime conversion as the ts_col can be string or numeric format. TODO: Comparison boundaries are confusing
        if (self.end_ts_col is not None):
            xtsv = xtsv \
                .not_eq_str(self.end_ts_col, "") \
                .filter(self.end_ts_col, __get_next_data_update_end_boundary_inner__) \
                .sort(self.end_ts_col)

        # select the output cols
        xtsv = xtsv.select(self.cols)

        # create combined data. TODO: Do we need distinct()
        xtsv_combined = tsv.merge_union([xtsv, local_xtsv_base])
        xtsv_new = xtsv_combined \
            .difference(local_xtsv_base)

        # apply additional filtering on the detect_ts
        if (self.end_ts_col is not None and self.end_ts_col in self.cols):
            # sort the combined data
            xtsv_combined = xtsv_combined.sort(self.end_ts_col)

        # update the base
        local_xtsv_base = xtsv_combined.last(self.rollover)

        # return
        return xtsv_new, local_xtsv_base

    def refresh_plot_data(self, xtsv):
        # check for empty
        if (xtsv.num_rows() > 0):
            # define new data
            mp = {}
            for c in self.cols:
                mp[c] = xtsv.col_as_array(c)

            # check if delta update is enabled
            if (self.delta_update_flag == True):
                # update
                self.sources[0].stream(mp, rollover = self.rollover)
            else:
                self.sources[0].data = mp

class ContinousGraphDataTable(ContinuousGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None, include_end_boundary = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path, include_end_boundary = include_end_boundary)

    def create_figure(self):
        table_columns = list([TableColumn(field = c, title = c) for c in self.cols])
        label = Div(text = "<h3>{}</h3>".format(self.title))
        table = DataTable(source = self.sources[0], columns = table_columns, width = self.width, height = self.height)
        return column(label, table)
        # return table

class ContinuousGraphSnapshot(BaseGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None, include_end_boundary = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path)
        self.include_end_boundary = include_end_boundary

    # create source
    def create_sources(self):
        # create ColumnDataSource, and define table columns
        mp = {}
        for c in self.cols:
            mp[c] = []

        # create source
        return [ColumnDataSource(mp)]

    def get_next_data_update(self, cur_end_ts, local_xtsv_base):
        # construct start and end dates
        sdate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts - self.interval - self.window)
        edate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts)

        # get the timeestamps
        sdate_ts = funclib.datetime_to_utctimestamp(sdate_str)
        edate_ts = funclib.datetime_to_utctimestamp(edate_str)

        # read base input
        xtsv = etl.scan_by_datetime_range(self.input_path_map["default"], sdate_str, edate_str, "output", num_par = 0, def_val_map = {})

        # check filter function on xtsv
        if (self.filter_transform_func is not None):
            xtsv = self.filter_transform_func(xtsv)

        # for each missing col, add to the data
        xtsv = xtsv.add_empty_cols_if_missing(self.cols)
        xtsv.add_empty_cols_if_missing([self.start_ts_col])
        xtsv.add_empty_cols_if_missing([self.end_ts_col])

        # check for empty
        if (xtsv.num_rows() == 0):
            return xtsv, local_xtsv_base

        def __get_next_data_update_end_boundary_inner__(t):
            if (self.include_end_boundary == True):
                return funclib.datetime_to_utctimestamp(t) >= sdate_ts and funclib.datetime_to_utctimestamp(t) <= edate_ts
            else:
                return funclib.datetime_to_utctimestamp(t) >= sdate_ts and funclib.datetime_to_utctimestamp(t) < edate_ts

        # apply the timestamp col filter. Call datetime conversion as the ts_col can be string or numeric format. TODO: Comparison boundaries are confusing
        if (self.end_ts_col is not None):
            xtsv = xtsv \
                .not_eq_str(self.end_ts_col, "") \
                .filter(self.end_ts_col, __get_next_data_update_end_boundary_inner__) \
                .sort(self.end_ts_col)

        # select the output cols
        xtsv = xtsv.select(self.cols)

        # create combined data. TODO: Do we need distinct()
        xtsv_combined = xtsv
        xtsv_new = xtsv_combined

        # apply additional filtering on the detect_ts
        if (self.end_ts_col is not None and self.end_ts_col in self.cols):
            # sort the combined data
            xtsv_combined = xtsv_combined.sort(self.end_ts_col)

        # update the base
        local_xtsv_base = xtsv_combined.last(self.rollover)

        # return
        return xtsv_new, local_xtsv_base

    def refresh_plot_data(self, xtsv):
        # check for empty
        if (xtsv.num_rows() > 0):
            # define new data
            mp = {}
            for c in self.cols:
                mp[c] = xtsv.col_as_array(c)

            self.sources[0].data = mp

class ContinousGraphSnapshotDataTable(ContinuousGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None, include_end_boundary = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path, include_end_boundary = include_end_boundary)

    def create_figure(self):
        table_columns = list([TableColumn(field = c, title = c) for c in self.cols])
        label = Div(text = "<h3>{}</h3>".format(self.title))
        table = DataTable(source = self.sources[0], columns = table_columns, width = self.width, height = self.height)
        return column(label, table)
        # return table

class ContinousGraphLineChart(ContinuousGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path)

    def create_figure(self):
        f = figure(width = self.width, height = self.height, title = self.title)
        f.line(x = "x", y = "y", source = self.sources[0])
        return f

class CategoricalGraph(BaseGraph):
    INTERVAL_DAYS = "INTERVAL_DAYS"
    INTERVAL_HOURS = "INTERVAL_HOURS"
    INTERVAL_MINUTES = "INTERVAL_MINUTES"
    INTERVAL_SECONDS = "INTERVAL_SECONDS"
    GROUP_COL = "__assign_time_slot_group_col_index__"

    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None, delta_update_flag = None,
        categories = None, xcol = None, ycol = None, color = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path)

        # store categories for histograms
        self.categories = categories
        self.xcol = xcol
        self.ycol = ycol
        self.color = color
        self.is_live = None

        # determine the x_range.
        # For end_date in the past, determine the time slots based on end_date, start_date and interval
        # For live end_date (None), determine the timeslots based on x-N hrs and use rollover
        if (funclib.datetime_to_utctimestamp(end_date_str) > funclib.get_utctimestamp_sec()):
            self.is_live = True
            self.time_slots = list([self.interval * i for i in range(self.rollover)])
            self.time_slots.reverse()

            self.time_slots_labels = []
            for t in self.time_slots:
                interval_granularity, interval_duration, interval_suffix = CategoricalGraph.__get_interval_granularity__(t)
                self.time_slots_labels.append("now{}{}".format(interval_duration, interval_suffix))
            utils.info("is_live = True: {}, {}".format(self.time_slots, self.time_slots_labels))
        else:
            self.is_live = False
            num_slots = int(math.ceil(funclib.datetime_to_utctimestamp(self.end_date_str) - funclib.datetime_to_utctimestamp(self.start_date_str)) / self.interval)
            self.time_slots = list([funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(self.start_date_str) + (i * self.interval)) for i in range(num_slots)])
            self.time_slots_labels = list([funclib.datestr_to_datetime(t).strftime("%Y-%m-%dT%H:%M:%S") for t in self.time_slots])
            utils.info("is_live = False: {}, {}".format(self.time_slots, self.time_slots_labels))

    # TODO: remove this. use predefined resolution value
    def __get_interval_granularity__(x):
        if (x == 0):
            return (CategoricalGraph.INTERVAL_SECONDS, "", "")
        elif (x % 86400 == 0):
            return (CategoricalGraph.INTERVAL_DAYS, str(-int(x / 86400)), "d")
        elif (x % 3600 == 0):
            return (CategoricalGraph.INTERVAL_HOURS, str(-int(x / 3600)), "h")
        elif (x % 60 == 0):
            return (CategoricalGraph.INTERVAL_MINUTES, str(-int(x / 60)), "m")
        else:
            return (CategoricalGraph.INTERVAL_SECONDS, str(-int(x)), "s")

    def create_sources(self):
        # create source with empty values
        mp = {}
        mp["time_slots"] = self.time_slots
        mp["time_slots_labels"] = self.time_slots_labels
        for category in self.categories:
            mp[category] = list([0 for i in self.time_slots])

        utils.info("create_source categorical: {}".format(mp))

        # return
        return [ColumnDataSource(mp)]

    # TODO: Add waiting window logic
    def get_next_data_update(self, cur_end_ts, local_xtsv_base):
        # construct start and end dates
        if (self.is_live == True):
            sdate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts - self.interval * self.rollover)
            edate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts)
        else:
            sdate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts - self.interval)
            edate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts)

        # debug
        utils.info("get_next_data_update: cur_end_ts: {}, sdate_str: {}, edate_str: {}".format(funclib.utctimestamp_to_datetime_str(cur_end_ts), sdate_str, edate_str))

        # get the timeestamps
        sdate_ts = funclib.datetime_to_utctimestamp(sdate_str)
        edate_ts = funclib.datetime_to_utctimestamp(edate_str)

        # read base input
        xtsv = etl.scan_by_datetime_range(self.input_path_map["default"], sdate_str, edate_str, "output", num_par = 0, def_val_map = {})

        # debug
        xtsv.noop(10, title = "after etl")

        # check filter function on xtsv
        if (self.filter_transform_func is not None):
            xtsv = self.filter_transform_func(xtsv)

        # debug
        xtsv.noop(10, title = "after etl filter transform")

        # for each missing col, add to the data
        xtsv = xtsv.add_empty_cols_if_missing(self.cols)

        # need a dense representation for live data
        if (self.is_live == True):
            xtsv.noop(4, title = "is live 1")
            # TODO: find concrete minute granularity
            time_values = list([funclib.utctimestamp_to_datetime_str(edate_ts - t) for t in self.time_slots])

            # take only the currently applicable time values
            cur_time_values = sorted(xtsv.values_in(self.end_ts_col, time_values).col_as_array_uniq(self.end_ts_col))

        # debug
        xtsv.noop(4, title = "after creating is_live version 2")

        # check for empty
        if (xtsv.num_rows() == 0):
            xtsv = xtsv.add_empty_cols_if_missing([CategoricalGraph.GROUP_COL])
            return xtsv, local_xtsv_base

        # select the output cols
        xtsv = xtsv.select(self.cols)

        def __assign_time_slot_group_index__(cur_start_str, cur_end_str, slots, slots_resolved):
            cur_start_ts, cur_end_ts = funclib.datetime_to_utctimestamp(cur_start_str), funclib.datetime_to_utctimestamp(cur_end_str)
            for i in range(len(slots_resolved)):
                slot_start_str, slot_end_str = slots_resolved[i]
                slot_start_ts, slot_end_ts = funclib.datetime_to_utctimestamp(slot_start_str), funclib.datetime_to_utctimestamp(slot_end_str)

                # include both boundaries for categorical
                if (cur_start_ts >= slot_start_ts and cur_end_ts <= slot_end_ts):
                    return i

            # debug
            utils.info("Failed to assign: cur_start: {}, cur_end: {}, slots: {}, slots_resolved: {}".format(cur_start_str, cur_end_str, slots, slots_resolved))
            return None

        def __assign_time_slot_group_value__(index, slots):
            return slots

        # apply the timestamp col filter. Call datetime conversion as the ts_col can be string or numeric format
        if (self.start_ts_col is not None and self.end_ts_col is not None):
            # do another round of aggregation to make sure the time slots have unique counts
            time_slots_resolved = None
            if (self.is_live == True):
                time_slots_resolved = list([(funclib.utctimestamp_to_datetime_str(edate_ts - self.time_slots[i] - self.interval),
                                             funclib.utctimestamp_to_datetime_str(edate_ts - self.time_slots[i])) for i in range(len(self.time_slots))])
            else:
                time_slots_resolved = list([(funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(t) - self.interval),
                                             funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(t))) for t in self.time_slots])

            # debug
            # print("get_next_data_update: time_slots_resolved: cur_end_ts: {}, sdate: {}, edate: {}, time_slots_resolved: {}".format(funclib.utctimestamp_to_datetime_str(cur_end_ts),
            #                                                                                                                     sdate_str, edate_str, time_slots_resolved))

            # remove invalid rows, and sum the columns correctly
            xtsv = xtsv \
                .noop(10, title = "xtsv ts_col1") \
                .not_eq_str(self.start_ts_col, "") \
                .not_eq_str(self.end_ts_col, "") \
                .not_eq_str(self.xcol, "") \
                .not_eq_str(self.ycol, "") \
                .noop(10, title = "xtsv ts_col before filtering timestamp value ts_col2") \
                .filter([self.start_ts_col, self.end_ts_col], lambda st_str, et_str: funclib.datetime_to_utctimestamp(st_str) >= sdate_ts and funclib.datetime_to_utctimestamp(et_str) <= edate_ts) \
                .noop(10, title = "xtsv ts_col after filtering timestamp value ts_col3") \
                .transform([self.start_ts_col, self.end_ts_col], lambda st_str, et_str: __assign_time_slot_group_index__(st_str, et_str, self.time_slots, time_slots_resolved), CategoricalGraph.GROUP_COL) \
                .noop(10, title = "xtsv ts_col4") \
                .aggregate([self.xcol, CategoricalGraph.GROUP_COL], [self.ycol], [sum]) \
                .noop(10, title = "xtsv ts_col5") \
                .rename("{}:sum".format(self.ycol), self.ycol) \
                .sort(CategoricalGraph.GROUP_COL) \
                .noop(10, title = "xtsv ts_col6")

        xtsv.add_empty_cols_if_missing([CategoricalGraph.GROUP_COL])

        # return
        return xtsv, xtsv

    def refresh_plot_data(self, xtsv):
        col_maps = xtsv.cols_as_map([self.xcol, CategoricalGraph.GROUP_COL], [self.ycol])
        # check for empty
        if (xtsv.num_rows() > 0):
            xtsv.noop(4, title = "refresh plot data catagorical")
            # define new data
            mp = {}
            mp["time_slots"] = self.time_slots
            mp["time_slots_labels"] = self.time_slots_labels # list(["{}:{}".format(t, random.randint(0, 100)) for t in self.time_slots_labels])

            # iterate through each category and create a dense representation
            for category in self.categories:
                col_map = xtsv.eq_str(self.xcol, category).cols_as_map([CategoricalGraph.GROUP_COL], [self.ycol])
                # print("refresh_plot_data: category: {}, col_map: {}".format(category, col_map))
                vs = []
                for slot_index in range(len(self.time_slots)):
                    slot_index = str(slot_index)
                    if (slot_index in col_map.keys()):
                        vs.append(float(col_map[slot_index]))
                    else:
                        vs.append(0)
                mp[category] = vs

            # replace entire source
            # print("refresh_plot_data categorical: {}".format(mp))
            self.sources[0].data = mp

class CategoricalBarChartTimeSeriesVBarStack(CategoricalGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None, delta_update_flag = None,
        categories = None, xcol = None, ycol = None, color = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag,
            categories = categories, xcol = xcol, ycol = ycol, color = color, output_path = output_path)

    def create_figure(self):
        label = Div(text = "<h3>{}</h3>".format(self.title))

        f = figure(x_range = self.time_slots_labels, width = self.width, height = self.height, tooltips="$name @$name")
        f.vbar_stack(self.categories, x = "time_slots_labels", source = self.sources[0], width = 0.9, legend_label = self.categories, color = self.color)
        f.y_range.start = 0
        f.x_range.range_padding = 0.1
        f.xgrid.grid_line_color = None
        f.axis.minor_tick_line_color = None
        f.outline_line_color = None
        f.legend.location = "top_left"
        f.legend.orientation = "vertical"
        f.xaxis.major_label_orientation = math.pi/4
        return column(label, f)
        # return f

class CategoricalBarChartTimeSeriesVLineStack(CategoricalGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None, delta_update_flag = None,
        categories = None, xcol = None, ycol = None, color = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag,
            categories = categories, xcol = xcol, ycol = ycol, color = color, output_path = output_path)

    def create_figure(self):
        label = Div(text = "<h3>{}</h3>".format(self.title))

        f = figure(x_range = self.time_slots_labels, width = self.width, height = self.height, tooltips="$name @{}: @$name".format("time_slots_labels"))
        f.vline_stack(self.categories, x = "time_slots_labels", source = self.sources[0], width = 0.9, legend_label = self.categories, color = self.color, line_width = 2)
        f.y_range.start = 0
        f.x_range.range_padding = 0.1
        f.xgrid.grid_line_color = None
        f.axis.minor_tick_line_color = None
        f.outline_line_color = None
        f.legend.location = "top_left"
        f.legend.orientation = "vertical"
        f.xaxis.major_label_orientation = math.pi/4
        return column(label, f)
        # return f

class Categorical2LevelGraph:
    def __init__(self, cols, categories1, categories2):
        pass

class CategoricalPieChart(CategoricalGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None, delta_update_flag = None,
        categories = None, xcol = None, ycol = None, color = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag,
            categories = categories, xcol = xcol, ycol = ycol, color = color, output_path = output_path)

    def create_sources(self):
        self.categories = sorted(self.categories)
        mp = {}
        mp[self.xcol] = [i for i in self.categories]
        mp["angle"] = [0 for i in self.categories]
        mp["color"] = self.color

        # return
        return [ColumnDataSource(mp)]

    def create_figure(self):
        label = Div(text = "<h3>{}</h3>".format(self.title))

        f = figure(x_range =(-0.5, 1.0), width = self.width, height = self.height)
        f.wedge(x = 0, y = 1, radius = 0.3, start_angle = cumsum("angle", include_zero = True), end_angle = cumsum("angle"), line_color = "white", fill_color = "color",
                legend_field = self.xcol, source = self.sources[0], tooltips="$name @{}: @$name".format(self.xcol))

        # wedge properties
        # f.axis.axis_label = None
        # f.axis.visible = False
        # f.grid.grid_line_color = None

        # return
        return column(label, f)

    def refresh_plot_data(self, xtsv):
        xtsv = xtsv \
            .noop(10, title = "pie chart input") \
            .select([self.xcol, self.ycol]) \
            .aggregate(self.xcol, [self.ycol], [sum]) \
            .rename("{}:sum".format(self.ycol), self.ycol) \
            .noop(10, title = "pie chart output") \
            .sort(self.xcol, all_numeric = False)

        # create default mp
        mp = {}
        mp[self.xcol] = [i for i in self.categories]
        mp["angle"] = [0 for i in self.categories]
        mp["color"] = self.color

        # take the total of values
        total = sum(xtsv.col_as_float_array(self.ycol))
        # print("Total: {}".format(total))

        # update the relevent categories
        cmap = xtsv.cols_as_map([self.xcol], [self.ycol])
        # print("cmap: {}".format(cmap))

        # iterate through categories and set the values
        for index in range(len(self.categories)):
            if (self.categories[index] in cmap.keys()):
                mp["angle"][index] = float(cmap[self.categories[index]]) / float(total) * 2 * pi

        # print
        # print("mp: {}".format(mp))

        # update
        self.sources[0].data = mp

class StaticGraphGeoIPMap(BaseGraph):
    def __init__(self, input_path_map = None, interval = None, window = None, cols = None, start_ts_col = None, end_ts_col = None,
        rollover = None, width = None, height = None, title = None, refresh_sec = None, start_date_str = None, end_date_str = None, filter_transform_func = None,
        delta_update_flag = None, output_path = None):

        # call base class to store all variables
        super().__init__(input_path_map = input_path_map, interval = interval, window = window, cols = cols, start_ts_col = start_ts_col, end_ts_col = end_ts_col,
            rollover = rollover, width = width, height = height, title = title, refresh_sec = refresh_sec, start_date_str = start_date_str, end_date_str = end_date_str,
            filter_transform_func = filter_transform_func, delta_update_flag = delta_update_flag, output_path = output_path)

        self.color_index_max = 10

    # create source
    def create_sources(self):
        # create ColumnDataSource, and define table columns
        mp = {}
        for c in self.cols:
            mp[c] = []

        # create source
        return [ColumnDataSource(mp)]

    def get_next_data_update(self, cur_end_ts, local_xtsv_base):
        print("Calling get_next_data_update: {}".format(funclib.utctimestamp_to_datetime_str(cur_end_ts)))

        sdate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts - self.interval * self.rollover)
        edate_str = etl.get_etl_datetime_str_from_ts(cur_end_ts)

        # debug
        # utils.info("get_next_data_update: cur_end_ts: {}, sdate_str: {}, edate_str: {}".format(funclib.utctimestamp_to_datetime_str(cur_end_ts), sdate_str, edate_str))

        # get the timeestamps
        sdate_ts = funclib.datetime_to_utctimestamp(sdate_str)
        edate_ts = funclib.datetime_to_utctimestamp(edate_str)

        # read base input
        xtsv = etl.scan_by_datetime_range(self.input_path_map["default"], sdate_str, edate_str, "output", num_par = 0, def_val_map = {})

        # boundary conditions
        if (xtsv.num_rows() == 0):
            return xtsv, xtsv

        # add the size. the proxy_ip and proxy_host values need to map from the source data
        xtsv = xtsv \
            .distinct() \
            .values_not_in("latitude", ["", "0.0"]) \
            .aggregate(["proxy_ip"], ["Count:sum", "end_ts"], [sum, max], collapse = False) \
            .rename("Count:sum:sum", "proxy_ip:count") \
            .rename("end_ts:max", "most_recent_ts") \
            .aggregate(["proxy_ip:latitude", "proxy_ip:longitude"], ["Count:sum", "proxy_ip"], [sum, funclib.uniq_count], collapse = False) \
            .rename("Count:sum:sum", "proxy_ip:latlon:count") \
            .noop(3, title = "proxy_ip:count") \
            .filter(["proxy_host:latitude", "proxy_host:longitude", "proxy_ip:latitude", "proxy_ip:longitude"], lambda ax,ay,rx,ry: ax != rx or ay != ry) \
            .transform(["most_recent_ts"], lambda x: (funclib.datetime_to_utctimestamp(x) - sdate_ts) / (edate_ts - sdate_ts), "circle_alpha") \
            .cap_max_inline("circle_alpha", "0.5")

        # xtsv.sample_n(10).select(["proxy_ip", "proxy_ip:count"]).show()

        # max_remote_ip4_count = max(xtsv.col_as_int_array("proxy_ip:count"))
        xtsv = xtsv \
            .transform("proxy_ip:uniq_count", lambda x: self.color_index_max if (int(x) >= self.color_index_max) else x, "color_index") \
            .transform("proxy_ip:latlon:count", lambda x: 10 + min(int(x) * 2, 60), "circle_size") \
            .drop_cols("proxy_ip:count") \
            .drop_cols("proxy_ip:latlon:count") \
            .noop(3, title = "circle_size")

        # debug
        xtsv.noop(10, title = "after etl")

        # check filter function on xtsv
        if (self.filter_transform_func is not None):
            xtsv = self.filter_transform_func(xtsv)

        # debug
        xtsv.noop(10, title = "after etl filter transform")

        # for each missing col, add to the data
        xtsv = xtsv.add_empty_cols_if_missing(self.cols)

        # return
        return xtsv, xtsv

    def refresh_plot_data(self, xtsv):
        # return if there is no data
        if (xtsv.num_rows() == 0):
            mp = {}
            for c in self.cols:
                mp[c] = []
            self.sources[0].data = mp
            return

        # debug
        # xtsv.show_transpose(3, title = "calling refresh_plot_data")

        # drop the columns that are going to be set again
        xtsv = xtsv \
            .drop_cols(["proxy_ip:coordinates", "proxy_ip:mercator", "proxy_ip:mercator_x", "proxy_ip:mercator_y"]) \
            .drop_cols(["proxy_host:coordinates", "proxy_host:mercator", "proxy_host:mercator_x", "proxy_host:mercator_y"]) \
            .drop_cols(["xs", "ys"])

        # create a data frame with floating data types wherever applicable
        df = xtsv.to_df(infer_data_types = True, no_infer_cols = ["node_id"])

        # Define coord as tuple (lat,lon)
        df['proxy_ip:coordinates'] = list(zip(df['proxy_ip:latitude'], df['proxy_ip:longitude']))
        df['proxy_host:coordinates'] = list(zip(df['proxy_host:latitude'], df['proxy_host:longitude']))

        # Obtain list of mercator coordinates
        mercators1 = [self.__x_coord__(x, y) for x, y in df['proxy_ip:coordinates'] ]
        mercators2 = [self.__x_coord__(x, y) for x, y in df['proxy_host:coordinates'] ]

        # Create mercator column in our df
        df['proxy_ip:mercator'] = mercators1
        df['proxy_host:mercator'] = mercators2

        # Split that column out into two separate cols - mercator_x and mercator_y
        df[['proxy_ip:mercator_x', 'proxy_ip:mercator_y']] = df['proxy_ip:mercator'].apply(pd.Series)
        df[['proxy_host:mercator_x', 'proxy_host:mercator_y']] = df['proxy_host:mercator'].apply(pd.Series)

        min_x = min(df["proxy_ip:mercator_x"])
        max_x = max(df["proxy_ip:mercator_x"])
        min_y = min(df["proxy_ip:mercator_y"])
        max_y = max(df["proxy_ip:mercator_y"])
        # print(min_x, max_x, min_y, max_y)

        # print("refresh: df: {}".format(df.columns))
        # print(df.head(5))

        # line segments for edges
        # xs = []
        # ys = []
        # for i in range(len(df)):
        #     xs.append([df[i]["proxy_host:mercator_x"], df[i]["proxy_ip:mercator_x"]])
        #     ys.append([df[i]["proxy_host:mercator_y"], df[i]["proxy_ip:mercator_y"]])

        # df["xs"] = xs
        # df["ys"] = ys
        df["xs"] = list(zip(df["proxy_host:mercator_x"], df["proxy_ip:mercator_x"]))
        df["ys"] = list(zip(df["proxy_host:mercator_y"], df["proxy_ip:mercator_y"]))

        # print(df.head(4))

        # create a map of series
        mp = {}
        for c in df.columns:
            mp[c] = df[c]

        # set source
        self.sources[0].data = mp

    def create_figure(self):
        label = Div(text = "<h3>{}</h3>".format(self.title))

        # Select tile set to use
        chosentile = get_provider(Vendors.CARTODBPOSITRON)

        # Choose palette
        palette = Category20[self.color_index_max]

        # Define color mapper - which column will define the colour of the data points
        low = 0
        high = self.color_index_max
        # color_mapper = linear_cmap(field_name = 'color_index', palette = palette, low = df['color_index'].min(), high = df['color_index'].max())
        color_mapper = linear_cmap(field_name = 'color_index', palette = palette, low = low, high = high)

        # Set tooltips - these appear when we hover over a data point in our map, very nifty and very useful
        nan_color = '#d9d9d9'
        tooltips = [("Count", "@color_index"), ("Alpha", "@circle_alpha"), ("IP", "@ip")]

        # Create figure with boundaries of the entire world
        min_x, max_x, min_y, max_y = -13624971.673499351, 16832321.97793506, -4011071.4166808245, 6895498.946934601

        # adjust
        min_x = 0.5 * min_x if (min_x >= 0) else 1.5 * min_x
        max_x = 1.5 * max_x if (max_x >= 0) else 0.5 * max_x
        min_y = 0.5 * min_y if (min_y >= 0) else 1.5 * min_y
        max_y = 1.5 * max_y if (max_y >= 0) else 0.5 * max_y

        # create figure
        p = figure(x_axis_type = "mercator", y_axis_type = "mercator",  x_axis_label = 'Longitude', y_axis_label = 'Latitude', tooltips = tooltips,
                   width = self.width, height = self.height, x_range = [min_x, max_x], y_range = [min_y, max_y])

        # Add map tile
        p.add_tile(chosentile)

        # Add points using mercator coordinates
        c1 = Circle(x = 'proxy_ip:mercator_x', y = 'proxy_ip:mercator_y', fill_color = color_mapper, size = "circle_size", fill_alpha = "circle_alpha")
        c2 = Circle(x = 'proxy_host:mercator_x', y = 'proxy_host:mercator_y', fill_color = "#ff0000", size = 20)
        c3 = Circle(x = 'proxy_host:mercator_x', y = 'proxy_host:mercator_y', fill_color = "#000000", size = 10)
        m1 = MultiLine(xs = "xs", ys = "ys", line_width = 1, line_dash = "dotted", line_alpha = "circle_alpha")
        p.add_glyph(self.sources[0], m1)
        p.add_glyph(self.sources[0], c1)
        p.add_glyph(self.sources[0], c2)
        p.add_glyph(self.sources[0], c3)

        #Defines color bar
        color_bar = ColorBar(color_mapper=color_mapper['transform'], formatter = NumeralTickFormatter(format='0.0[0000]'), label_standoff = 13, width = 8, location = (0,0))

        # Set color_bar location
        p.add_layout(color_bar, 'right')

        return column(label, p)

    def set_width_height(self, xwidth, xheight):
        self.fig.width = xwidth
        self.fig.height = xheight

    def __x_coord__(self, x, y):
        lat = x
        lon = y

        r_major = 6378137.000
        x = r_major * np.radians(lon)
        scale = x / lon
        y = 180.0 / np.pi * np.log(np.tan(np.pi / 4.0 + lat * (np.pi / 180.0) / 2.0)) * scale
        return (x, y)

def info(msg):
    # utils.info("utils: " + msg)
    print("print: " + msg)
    # logging.info("logging: " + msg)

def __demo_plot7_create_source__(cols):
        # create ColumnDataSource, and define table columns
        mp = {}
        for c in cols:
            mp[c] = xtsv.col_as_array(c)

        # create source
        source = ColumnDataSource(mp)

        return source

def __demo_plot7_data_table_get_data__(input_path, cols, ts_col, cur_ts, interval, window, rollover, filter_transform_func, local_xtsv_base):
    print("__plot7_data_table_update__ get data: {}".format(funclib.utctimestamp_to_datetime_str(cur_ts)))

    # define nonlocal
    # nonlocal local_xtsv_base

    # construct start and end dates
    sdate_str = etl.get_etl_datetime_str_from_ts(cur_ts - interval - window)
    edate_str = etl.get_etl_datetime_str_from_ts(cur_ts)
    sdate_ts = funclib.datetime_to_utctimestamp(sdate_str)
    edate_ts = funclib.datetime_to_utctimestamp(edate_str)

    # read base input
    xtsv = etl.scan_by_datetime_range(input_path, sdate_str, edate_str, "output", num_par = 0, def_val_map = {})

    # check filter function on xtsv
    if (filter_transform_func is not None):
        xtsv = filter_transform_func(xtsv)

    # for each missing col, add to the data
    xtsv = xtsv.add_empty_cols_if_missing(cols)

    # check for empty
    if (xtsv.num_rows() == 0):
        return xtsv, local_xtsv_base

    # select the output cols
    xtsv = xtsv.select(cols)

    # apply the timestamp col filter. Call datetime conversion as the ts_col can be string or numeric format
    if (ts_col is not None):
        xtsv = xtsv \
            .not_eq_str(ts_col, "") \
            .filter(ts_col, lambda t: funclib.datetime_to_utctimestamp(t) >= sdate_ts and funclib.datetime_to_utctimestamp(t) < edate_ts) \
            .sort(ts_col)

    # create combined data
    xtsv_combined = tsv.merge_union([xtsv, local_xtsv_base])
    xtsv_new = xtsv_combined.difference(local_xtsv_base)

    # apply additional filtering on the detect_ts
    if (ts_col is not None):
        # sort the combined data
        xtsv_combined = xtsv_combined.sort(ts_col)

    # update the base
    local_xtsv_base = xtsv_combined.last(rollover)

    # return
    return xtsv_new, local_xtsv_base


def demo_plot7_data_table(input_path = None, interval = 30, window = 180, cols = None, ts_col = None,
    rollover = 100, width = 1400, height = 300, refresh_sec = 10, start_date_str = None, end_date_str = None, filter_transform_func = None,
    fig = None, source = None):

    def __demo_plot7_data_table__(doc):
        local_xtsv_base = None
        local_end_ts = funclib.get_utctimestamp_sec() if (start_date_str is None) else funclib.datetime_to_utctimestamp(start_date_str)
        local_finished_ts = None if (end_date_str is None) else funclib.datetime_to_utctimestamp(end_date_str)

        # read data slice
        xtsv = tsv.new_with_cols(cols)


        # define table_columns
        # table_columns = list([TableColumn(field = c, title = c) for c in cols])

        # create data table
        # data_table = DataTable(source = source, columns = table_columns, width = width, height = height)
        # fig = plot7_create_figure(figure_name, source, cols, **figure_creation_props)
        # fig.source = source
        local_xtsv_base = xtsv

        # periodic callback function
        def __demo_plot7_data_table_update__():
            # define nonlocal for update
            nonlocal local_end_ts
            nonlocal local_finished_ts
            nonlocal local_xtsv_base
            print("Calling update: {}".format(funclib.utctimestamp_to_datetime_str(local_end_ts)))

            # check if local_end_ts is more that current timestamp
            if ((local_finished_ts is None and local_end_ts > funclib.get_utctimestamp_sec()) or
                (local_finished_ts is not None and local_end_ts > local_finished_ts)):
                # ignore
                return

            # get new data
            xtsv, local_xtsv_base = __plot7_data_table_get_data__(input_path, cols, ts_col, local_end_ts, interval, window, rollover, filter_transform_func, local_xtsv_base)

            # update local_end_ts
            local_end_ts = local_end_ts + interval

            print("__plot7_data_table_update__ num_rows: {}".format(xtsv.num_rows()))

            # check for empty
            if (xtsv.num_rows() > 0):
                # define new data
                mp = {}
                for c in cols:
                    mp[c] = xtsv.col_as_array(c)

                # update
                source.stream(mp, rollover = rollover)

        # show(data_table)
        info("calling add_root")
        doc.add_periodic_callback(__plot7_data_table_update__, refresh_sec * 1000)
        doc.add_root(fig)

    # main function
    output_notebook()
    show(__demo_plot7_data_table__)

def demo_plot1_circle():
    # directive for output to notebook
    output_notebook()

    # create figure
    p = figure(plot_width = 400, plot_height = 400)

    # add a circle renderer with
    # size, color and alpha
    p.circle([1, 2, 3, 4, 5], [4, 7, 1, 6, 3],
         size = 10, color = "navy", alpha = 0.5)

    # show the results
    show(p)

def demo_plot2_line():
    # directive for output to notebook
    output_notebook()

    # create figure
    p = figure(plot_width = 400, plot_height = 400)

    # add a line renderer
    p.line([1, 2, 3, 4, 5], [3, 1, 2, 6, 5],
        line_width = 2, color = "green")

    # show the results
    show(p)

def __demo_plot3_multiple__(doc):
    # This is a Bokeh server app. To function, it must be run using the
    # Bokeh server ath the command line:
    #
    #     bokeh serve --show patch_app.py
    #
    # Running "python patch_app.py" will NOT work.
    import numpy as np

    # from bokeh.io import curdoc

    from bokeh.layouts import gridplot
    from bokeh.models import ColumnDataSource
    from bokeh.plotting import figure

    # CDS with "typical" scalar elements
    x = np.random.uniform(10, size=500)
    y = np.random.uniform(10, size=500)
    color = ["navy"]*500
    color[:200] = ["firebrick"]*200
    source = ColumnDataSource(data=dict(x=x, y=y, color=color))

    p = figure(width=400, height=400)
    p.circle('x', 'y', alpha=0.6, size=8, color="color", source=source)

    # CDS with 1d array elements
    x = np.linspace(0, 10, 200)
    y0 = np.sin(x)
    y1 = np.cos(x)
    source1d = ColumnDataSource(data=dict(xs=[x, x], ys=[y0, y1], color=["olive", "navy"]))

    p1d = figure(width=400, height=400)
    p1d.multi_line('xs', 'ys', alpha=0.6, line_width=4, color="color", source=source1d)

    # CDS with 2d image elements
    N = 200
    img = np.empty((N,N), dtype=np.uint32)
    view = img.view(dtype=np.uint8).reshape((N, N, 4))
    for i in range(N):
        for j in range(N):
            view[i, j, :] = [int(j/N*255), int(i/N*255), 158, 255]
    source2d = ColumnDataSource(data=dict(img=[img]))

    p2d = figure(width=400, height=400, x_range=(0,10), y_range=(0,10))
    p2d.image_rgba(image='img', x=0, y=0, dw=10, dh=10, source=source2d)

    def __demo_plot3_multiple_update__():

        # update some items in the "typical" CDS column
        s = slice(100)
        new_x = source.data['x'][s] + np.random.uniform(-0.1, 0.1, size=100)
        new_y = source.data['y'][s] + np.random.uniform(-0.2, 0.2, size=100)
        source.patch({ 'x' : [(s, new_x)], 'y' : [(s, new_y)] })

        # update a single point of the 1d multi-line data
        i = np.random.randint(200)
        new_y = source1d.data['ys'][0][i] + (0.2 * np.random.random()-0.1) #nosec
        source1d.patch({ 'ys' : [([0, i], [new_y])]})

        # update five rows of the 2d image data at a time
        s1, s2 = slice(50, 151, 20), slice(None)
        index = [0, s1, s2]
        new_data = np.roll(source2d.data['img'][0][s1, s2], 2, axis=1).flatten()
        source2d.patch({ 'img' : [(index, new_data)] })

    # curdoc().add_periodic_callback(update, 50)
    # curdoc().add_root(gridplot([[p, p1d, p2d]]))
    doc.add_periodic_callback(__plot3_multiple_update__, 1000)
    doc.add_root(gridplot([[p, p1d, p2d]]))

def demo_plot3_multiple():
    # reset_output()
    output_notebook()
    show(__demo_plot3_multiple__)

# num_rows = 0
def __demo_plot4_data_table__(doc):
    from datetime import date
    from random import randint
    from omigo_core import tsv, utils

    from bokeh.plotting import show, figure
    from bokeh.models import ColumnDataSource, DataTable, DateFormatter, TableColumn

    xtsv = tsv.read("data/iris.tsv").add_seq_num("sno")
    num_rows = 1

    # define source data
    xtsv1 = xtsv.take(num_rows)
    source = ColumnDataSource({
            "sno": xtsv1.col_as_array("sno"),
            "class": xtsv1.col_as_array("class"),
            "petal_width": xtsv1.col_as_array("petal_width"),
            "sepal_width": xtsv1.col_as_array("sepal_width")
    })

    # define columns
    columns = [
        TableColumn(field = "sno", title = "sno"),
        TableColumn(field = "class", title = "class"),
        TableColumn(field = "petal_width", title = "petal_width"),
        TableColumn(field = "sepal_width", title = "sepal_width")
    ]
    data_table = DataTable(source=source, columns=columns, width=400, height=280)

    # periodic callback function
    def __demo_plot4_data_table_update__():
        nonlocal num_rows
        xtsv2 = xtsv.skip_rows(num_rows).take(1)
        num_rows = num_rows + 1
        new_data = {
            "sno": xtsv2.col_as_array("sno"),
            "class": xtsv2.col_as_array("class"),
            "petal_width": xtsv2.col_as_array("petal_width"),
            "sepal_width": xtsv2.col_as_array("sepal_width")
        }
        source.stream(new_data, rollover = 5)

    # show(data_table)
    doc.add_periodic_callback(__plot4_data_table_update__, 1000)
    doc.add_root(data_table)

def demo_plot4_data_table():
    output_notebook()
    x = show(__demo_plot4_data_table__)

def __demo_plot6_linking_multiple__(doc):
    from bokeh.io import output_notebook, show
    from bokeh.layouts import gridplot
    from bokeh.plotting import figure

    x = list(range(11))
    y0 = x
    y1 = [10-xx for xx in x]
    y2 = [abs(xx-5) for xx in x]

    # create a new plot
    s1 = figure(width=250, height=250, title=None)
    s1.circle(x, y0, size=10, color="navy", alpha=0.5)

    # create a new plot and share both ranges
    s2 = figure(width=250, height=250, x_range=s1.x_range, y_range=s1.y_range, title=None)
    s2.triangle(x, y1, size=10, color="firebrick", alpha=0.5)

    # create a new plot and share only one range
    s3 = figure(width=250, height=250, x_range=s1.x_range, title=None)
    s3.square(x, y2, size=10, color="olive", alpha=0.5)

    p = gridplot([[s1, s2, s3]], toolbar_location=None)

    # show the results
    doc.add_root(p)

def demo_plot6_linking_multiple():
    output_notebook()
    show(__demo_plot6_linking_multiple__)

def demo_plot8_graph():
    import math
    from bokeh.plotting import figure
    from bokeh.models import GraphRenderer, Ellipse, StaticLayoutProvider
    from bokeh.palettes import Spectral8
    from bokeh.models import BoxSelectTool

    # list the nodes and initialize a plot
    N = 8
    node_indices = list(range(N))

    plot = figure(title="Graph layout demonstration", x_range=(-1.1,1.1),
                  y_range=(-1.1,1.1), toolbar_location="below", tools="pan,lasso_select,box_select", active_drag="lasso_select")
    #plot.add_tools(BoxSelectTool(dimensions="width"))

    graph = GraphRenderer()

    # replace the node glyph with an ellipse
    # set its height, width, and fill_color
    graph.node_renderer.glyph = Ellipse(height=0.1, width=0.2,
                                        fill_color="fill_color")

    # assign a palette to ``fill_color`` and add it to the data source
    graph.node_renderer.data_source.data = dict(
        index=node_indices,
        fill_color=Spectral8)

    # add the rest of the assigned values to the data source
    graph.edge_renderer.data_source.data = dict(
        start=[0]*N,
        end=node_indices)

    # generate ellipses based on the ``node_indices`` list
    circ = [i*2*math.pi/8 for i in node_indices]

    # create lists of x- and y-coordinates
    x = [math.cos(i) for i in circ]
    y = [math.sin(i) for i in circ]

    # convert the ``x`` and ``y`` lists into a dictionary of 2D-coordinates
    # and assign each entry to a node on the ``node_indices`` list
    graph_layout = dict(zip(node_indices, zip(x, y)))

    # use the provider model to supply coourdinates to the graph
    graph.layout_provider = StaticLayoutProvider(graph_layout=graph_layout)

    # render the graph
    plot.renderers.append(graph)

    # specify the name of the output file
    output_file('graph.html')

    # display the plot
    show(plot)

def demo_plot9_graph():
    import math

    from bokeh.io import output_file, show
    from bokeh.models import Ellipse, GraphRenderer, StaticLayoutProvider
    from bokeh.palettes import Spectral8
    from bokeh.plotting import figure

    N = 8
    node_indices = list(range(N))

    plot = figure(title="Graph Layout Demonstration", x_range=(-1.1,1.1), y_range=(-1.1,1.1),
                  tools="", toolbar_location=None)

    graph = GraphRenderer()

    graph.node_renderer.data_source.add(node_indices, 'index')
    graph.node_renderer.data_source.add(Spectral8, 'color')
    graph.node_renderer.glyph = Ellipse(height=0.1, width=0.2, fill_color="color")

    graph.edge_renderer.data_source.data = dict(
        start=[0]*N,
        end=node_indices)

    ### start of layout code
    circ = [i*2*math.pi/8 for i in node_indices]
    x = [math.cos(i) for i in circ]
    y = [math.sin(i) for i in circ]
    graph_layout = dict(zip(node_indices, zip(x, y)))
    graph.layout_provider = StaticLayoutProvider(graph_layout=graph_layout)

    ### Draw quadratic bezier paths
    def bezier(start, end, control, steps):
        return [(1-s)**2*start + 2*(1-s)*s*control + s**2*end for s in steps]

    xs, ys = [], []
    sx, sy = graph_layout[0]
    steps = [i/100. for i in range(100)]
    for node_index in node_indices:
        ex, ey = graph_layout[node_index]
        xs.append(bezier(sx, ex, 0, steps))
        ys.append(bezier(sy, ey, 0, steps))
    graph.edge_renderer.data_source.data['xs'] = xs
    graph.edge_renderer.data_source.data['ys'] = ys

    plot.renderers.append(graph)

    output_file("graph.html")
    show(plot)

def demo_plot10_graph():
    import networkx as nx

    from bokeh.io import output_file, show
    from bokeh.models import (BoxZoomTool, Circle, HoverTool,
                              MultiLine, Plot, Range1d, ResetTool)
    from bokeh.palettes import Spectral4
    from bokeh.plotting import from_networkx

    # Prepare Data
    G = nx.karate_club_graph()

    SAME_CLUB_COLOR, DIFFERENT_CLUB_COLOR = "black", "red"
    edge_attrs = {}

    for start_node, end_node, _ in G.edges(data=True):
        edge_color = SAME_CLUB_COLOR if G.nodes[start_node]["club"] == G.nodes[end_node]["club"] else DIFFERENT_CLUB_COLOR
        edge_attrs[(start_node, end_node)] = edge_color

    nx.set_edge_attributes(G, edge_attrs, "edge_color")

    # Show with Bokeh
    plot = Plot(width=400, height=400,
                x_range=Range1d(-1.1, 1.1), y_range=Range1d(-1.1, 1.1))
    plot.title.text = "Graph Interaction Demonstration"

    # node_hover_tool = HoverTool(tooltips=[("index", "@index"), ("club", "@club")])
    # plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())

    graph_renderer = from_networkx(G, nx.spring_layout, scale=1, center=(0, 0))

    graph_renderer.node_renderer.glyph = Circle(size=15, fill_color=Spectral4[0])
    graph_renderer.edge_renderer.glyph = MultiLine(line_color="edge_color", line_alpha=0.8, line_width=1)
    plot.renderers.append(graph_renderer)

    output_file("interactive_graphs.html")
    show(plot)

def demo_plot11_graph_spring():
    import networkx as nx
    import pandas as pd
    from bokeh.models import Plot, ColumnDataSource, Range1d, Circle,MultiLine
    from bokeh.io import show, output_file
    from bokeh.palettes import Viridis
    from bokeh.plotting import from_networkx

    #define graph
    source = ['A', 'A', 'A','a','B','B','B','b']
    target = ['a', 'B','b','b','a','b','A','a']
    weight = [1,-1000,1,1,1,1, -1000, 1]
    df = pd.DataFrame([source,target,weight])
    df = df.transpose()
    df.columns = ['source','target','weight']
    G=nx.from_pandas_edgelist(df) # function signature changes

    #set node attributes
    node_color = {'A':Viridis[10][0], 'B':Viridis[10][9],'a':Viridis[10][4],'b':Viridis[10][4]}
    node_size = {'A':50, 'B':40,'a':10,'b':10}
    node_initial_pos = {'A':(-0.5,0), 'B':(0.5,0),'a':(0,0.25),'b':(0,-0.25)}
    nx.set_node_attributes(G,  node_color, name='node_color') # function signature changes
    nx.set_node_attributes(G,  node_size, name='node_size') # function signature changes
    nx.set_node_attributes(G,  node_initial_pos, name='node_initial_pos') # function signature changes

    #source with node color, size and initial pos (perhaps )
    source = ColumnDataSource(pd.DataFrame.from_dict({k:v for k,v in G.nodes(data=True)}, orient='index'))

    plot = Plot(plot_width=400, plot_height=400,
                x_range=Range1d(-1.1,1.1), y_range=Range1d(-1.1,1.1))

    graph_renderer = from_networkx(G, nx.spring_layout, scale=0.5, center=(0,0), pos=node_initial_pos)

    #style
    graph_renderer.node_renderer.data_source = source
    graph_renderer.node_renderer.glyph = Circle(fill_color = 'node_color',size = 'node_size', line_color = None)

    graph_renderer.edge_renderer.glyph = MultiLine(line_color="#CCCCCC", line_alpha=0.8, line_width=5)


    plot.renderers.append(graph_renderer)
    output_file('test.html')

    show(plot)

def demo_plot12_bar_chart():
    def __demo_plot12_bar_chart__(doc):
        from bokeh.io import output_file, show
        from bokeh.models import ColumnDataSource, FactorRange
        from bokeh.plotting import figure

        fruits = ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']
        counts = list([len(t) for t in fruits])

        # this creates [ ("Apples", "2015"), ("Apples", "2016"), ("Apples", "2017"), ("Pears", "2015), ... ]
        mp = dict(x = fruits, counts = counts)
        source = ColumnDataSource(data=mp)

        p = figure(x_range=fruits, height=250, title="Fruit counts", toolbar_location=None, tools="")

        p.vbar(x = 'x', top = 'counts', width=0.9, source=source)

        p.y_range.start = 0
        p.x_range.range_padding = 0.1
        p.xaxis.major_label_orientation = 1
        p.xgrid.grid_line_color = None

        non_local_counter = 0
        def __demo_plot12_bar_chart_update__():
            nonlocal non_local_counter
            non_local_counter = non_local_counter + 1

            new_mp = dict(x = fruits, counts = utils.merge_arrays([counts[1:], counts[0:1]]))

            source.stream(new_mp, rollover = 10)

        # show(data_table)
        doc.add_periodic_callback(__plot12_bar_chart_update__, 2000)
        doc.add_root(p)

    output_notebook()
    show(__demo_plot12_bar_chart__)

def demo_plot13_time_series():
    def __demo_plot13_time_series__(doc):
        import numpy as np

        from bokeh.layouts import gridplot
        from bokeh.plotting import figure, show
        from bokeh.sampledata.stocks import AAPL, GOOG, IBM, MSFT
        from bokeh.models import ColumnDataSource

        def datetime(x, start, end):
            return np.array(x[start:end], dtype=np.datetime64)

        p1 = figure(x_axis_type="datetime", title="Stock Closing Prices")
        p1.xaxis.axis_label = 'Date'
        p1.yaxis.axis_label = 'Price'

        data = {
            "x_values": datetime(AAPL['date'], 0, 10),
            "y_values": AAPL['adj_close'][0:10]
        }
        source = ColumnDataSource(data = data)

        p1.line(x = "x_values", y = "y_values", color='#A6CEE3', legend_label='AAPL', source = source)
        p1.legend.location = "top_left"

        non_local_counter = 10
        def __demo_plot13_time_series_update__():
            nonlocal non_local_counter
            non_local_counter = non_local_counter
            start = non_local_counter
            end = non_local_counter + 10
            non_local_counter = end

            new_mp = {
                "x_values": datetime(AAPL['date'], start, end),
                "y_values": AAPL['adj_close'][start:end]
            }

            print("doing update: {}".format(non_local_counter))
            source.stream(new_mp, rollover = 1000)

        doc.add_periodic_callback(__plot13_time_series_update__, 1000)
        doc.add_root(p1)

    output_notebook()
    show(__demo_plot13_time_series__)

def demo_plot14_index_filter_box_select():
    def __demo_plot14_index_filter_box_select__(doc):
        from bokeh.layouts import gridplot
        from bokeh.models import CDSView, ColumnDataSource, IndexFilter
        from bokeh.plotting import figure, show

        source = ColumnDataSource(data=dict(x=[1, 2, 3, 4, 5], y=[1, 2, 3, 4, 5]))
        view = CDSView(source=source, filters=[IndexFilter(source.selected.indices)])

        tools = ["box_select", "hover", "reset"]
        p = figure(height=300, width=300, tools=tools)
        p.circle(x="x", y="y", size=10, hover_color="red", source=source)

        p_filtered = figure(height=300, width=300, tools=tools)
        p_filtered.circle(x="x", y="y", size=10, hover_color="red", source=source, view=view)

        g = gridplot([[p, p_filtered]])

        doc.add_root(g)

    output_notebook()
    show(__demo_plot14_index_filter_box_select__)

def demo_plot15_customjs_slider():
    def __demo_plot_customjs_slider__(doc):
        from bokeh.layouts import column
        from bokeh.models import ColumnDataSource, CustomJS, Slider
        from bokeh.plotting import Figure, output_file, show

        x = [x*0.005 for x in range(0, 200)]
        y = x

        source = ColumnDataSource(data=dict(x=x, y=y))

        plot = Figure(width=400, height=400)
        plot.line('x', 'y', source=source, line_width=3, line_alpha=0.6)

        callback = CustomJS(args=dict(source=source), code="""
            const data = source.data;
            const f = cb_obj.value
            const x = data['x']
            const y = data['y']
            for (let i = 0; i < x.length; i++) {
                y[i] = Math.pow(x[i], f)
            }
            source.change.emit();
        """)

        slider = Slider(start=0.1, end=4, value=1, step=.1, title="power")
        slider.js_on_change('value', callback)

        layout = column(slider, plot)

        doc.add_root(layout)

    output_notebook()
    show(__demo_plot_customjs_slider__)

def demo_plot16_customjs_indices():
    def __demo_plot16_customjs_indices__(doc):
        from random import random

        from bokeh.layouts import row
        from bokeh.models import ColumnDataSource, CustomJS
        from bokeh.plotting import figure, output_file, show

        output_file("callback.html")

        x = [random() for x in range(500)] #nosec
        y = [random() for y in range(500)] #nosec

        s1 = ColumnDataSource(data=dict(x=x, y=y))
        p1 = figure(width=400, height=400, tools="lasso_select", title="Select Here")
        p1.circle('x', 'y', source=s1, alpha=0.6)

        s2 = ColumnDataSource(data=dict(x=[], y=[]))
        p2 = figure(width=400, height=400, x_range=(0, 1), y_range=(0, 1),
                    tools="", title="Watch Here")
        p2.circle('x', 'y', source=s2, alpha=0.6)

        s1.selected.js_on_change('indices', CustomJS(args=dict(s1=s1, s2=s2), code="""
                const inds = cb_obj.indices;
                const d1 = s1.data;
                const d2 = s2.data;
                d2['x'] = []
                d2['y'] = []
                for (let i = 0; i < inds.length; i++) {
                    d2['x'].push(d1['x'][inds[i]])
                    d2['y'].push(d1['y'][inds[i]])
                }
                s2.change.emit();
            """)
        )

        layout = row(p1, p2)

        doc.add_root(layout)

    output_notebook()
    show(__demo_plot16_customjs_indices__)

def demo_plot30_pie():
    x = {
        'United States': 157,
        'United Kingdom': 93,
        'Japan': 89,
        'China': 63,
        'Germany': 44,
        'India': 42,
        'Italy': 40,
        'Australia': 35,
        'Brazil': 32,
        'France': 31,
        'Taiwan': 31,
        'Spain': 29
    }

    data = pd.Series(x).reset_index(name='value').rename(columns={'index': 'country'})
    print(data)
    data['angle'] = data['value']/data['value'].sum() * 2*pi
    data['color'] = Category20c[len(x)]

    p = figure(height=350, title="Pie Chart", toolbar_location=None,
               tools="hover", tooltips="@country: @value", x_range=(-0.5, 1.0))

    p.wedge(x=0, y=1, radius=0.4,
            start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
            line_color="white", fill_color='color', legend_field='country', source=data)

    p.axis.axis_label = None
    p.axis.visible = False
    p.grid.grid_line_color = None

    output_notebook()
    show(p)

def demo_plot20_ip2location(df):
    def __demo_plot20_ip2location__(doc):
        # Define function to switch from lat/long to mercator coordinates
        def x_coord(x, y):

            lat = x
            lon = y

            r_major = 6378137.000
            x = r_major * np.radians(lon)
            scale = x/lon
            y = 180.0/np.pi * np.log(np.tan(np.pi/4.0 +
                lat * (np.pi/180.0)/2.0)) * scale
            return (x, y)

        # Define coord as tuple (lat,long)
        df['coordinates'] = list(zip(df['latitude'], df['longitude']))


        # Obtain list of mercator coordinates
        mercators = [x_coord(x, y) for x, y in df['coordinates'] ]

        # Create mercator column in our df
        df['mercator'] = mercators

        # Split that column out into two separate cols - mercator_x and mercator_y
        df[['mercator_x', 'mercator_y']] = df['mercator'].apply(pd.Series)

        # edges mapping
        xs_ids = [[int(i)] for i in df["start"]]
        ys_ids = [[int(i)] for i in df["end"]]

        # line segments for edges
        xs = []
        ys = []
        for i in range(len(xs_ids)):
            xs_id = xs_ids[i]
            ys_id = ys_ids[i]
            xs.append([df.iloc[xs_id[0]]["mercator_x"], df.iloc[ys_id[0]]["mercator_x"]])
            ys.append([df.iloc[xs_id[0]]["mercator_y"], df.iloc[ys_id[0]]["mercator_y"]])

        df["xs"] = xs
        df["ys"] = ys

        # Select tile set to use
        chosentile = get_provider(Vendors.CARTODBPOSITRON)

        # Choose palette
        palette = PRGn[11]

        # Tell Bokeh to use df as the source of the data
        source = ColumnDataSource(data = df)

        # Define color mapper - which column will define the colour of the data points
        color_mapper = linear_cmap(field_name = 'color_index', palette = palette, low = df['color_index'].min(), high = df['color_index'].max())

        # Set tooltips - these appear when we hover over a data point in our map, very nifty and very useful
        nan_color = '#d9d9d9'
        tooltips = [("aid","@node_id"), ("IP","@ip")]

        # Create figure
        min_x = min(df["mercator_x"])
        max_x = max(df["mercator_x"])
        min_y = min(df["mercator_y"])
        max_y = max(df["mercator_y"])

        # This is needed to prevent update() from rescaling the axis
        min_x = 0.8*min_x if (min_x >= 0) else 1.2*min_x
        max_x = 1.2*max_x if (max_x >= 0) else 0.8*max_x
        min_y = 0.8*min_y if (min_y >= 0) else 1.2*min_y
        max_y = 1.2*max_y if (max_y >= 0) else 0.8*max_y

        # print(min_x, min_y, max_x, max_y)

        p = figure(title = 'Lateral Movement', x_axis_type="mercator", y_axis_type="mercator",
                   x_axis_label = 'Longitude', y_axis_label = 'Latitude', tooltips = tooltips, width = 1300,
                   x_range = [min_x, max_x], y_range = [min_y, max_y])

        # Add map tile
        p.add_tile(chosentile)

        # Add points using mercator coordinates
        p.multi_line(xs = "xs", ys = "ys", source = source, line_width = 2)
        p.circle(x = 'mercator_x', y = 'mercator_y', color = color_mapper, source=source, size=25, fill_alpha = 0.7)

        #Defines color bar
        color_bar = ColorBar(color_mapper=color_mapper['transform'],
                             formatter = NumeralTickFormatter(format='0.0[0000]'),
                             label_standoff = 13, width=8, location=(0,0))

        # Set color_bar location
        p.add_layout(color_bar, 'right')

        def __update__():
            xs_org = source.data["xs"]
            ys_org = source.data["ys"]

            xs_org[0].append(-1.362767e+07)
            xs_org[0].append(-1.334192e+07)
            ys_org[0].append(4.547679e+06)
            ys_org[0].append(5.980402e+06)

            source.data["xs"] = xs_org
            source.data["ys"] = ys_org

        # doc.add_periodic_callback(__update__, 5000)
        doc.add_root(p)

    output_notebook()
    show(__demo_plot20_ip2location__)

def demo_plot12_xtsv_bar_chart():
    def __demo_plot12_xtsv_bar_chart__(doc):
        from bokeh.io import output_file, show
        from bokeh.models import ColumnDataSource, FactorRange
        from bokeh.plotting import figure

        fruits = ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']
        counts = list([len(t) for t in fruits])

        # this creates [ ("Apples", "2015"), ("Apples", "2016"), ("Apples", "2017"), ("Pears", "2015), ... ]
        mp = dict(fruits = fruits, counts = counts)
        source = ColumnDataSource(data=mp)

        # p = figure(x_range = fruits, height=250, title="Fruit counts", toolbar_location=None, tools="")
        p = figure(height=250, title="Fruit counts", toolbar_location="above", tools="hover,save,pan,box_zoom,reset,wheel_zoom,tap")

        p.vbar(x = "fruits", top = "counts", width=0.9, source=source)

        p.y_range.start = 0
        p.x_range.range_padding = 0.1
        p.xaxis.major_label_orientation = 1
        p.xgrid.grid_line_color = None
        p.axis.minor_tick_line_color = None
        p.outline_line_color = None
        p.xaxis.axis_label = 'Month'
        p.yaxis.axis_label = 'Average Crimes'
        p.select_one(HoverTool).tooltips = [
            ('month', '@fruits'),
            ('Number of crimes', '@counts'),
        ]

        non_local_counter = 0
        def __demo_plot12_xtsv_bar_chart_update__():
            nonlocal non_local_counter
            non_local_counter = non_local_counter + 1

            new_mp = dict(fruits = fruits, counts = utils.merge_arrays([counts[1:], counts[0:1]]))

            source.stream(new_mp, rollover = 10)

        # show(data_table)
        # doc.add_periodic_callback(__plot12_xtsv_bar_chart_update__, 2000)
        doc.add_root(p)

    output_notebook()
    show(__demo_plot12_xtsv_bar_chart__)


# pylint: disable=too-many-locals, too-many-statements
# Modified version of this: https://github.com/microsoft/msticpy/blob/main/msticpy/nbtools/process_tree.py
def plot_process_tree_omigo(  # noqa: MC0001
    data: pd.DataFrame,
    schema: ProcSchema = None,
    output_var: str = None,
    legend_col: str = None,
    show_table: bool = False,
    **kwargs,
) -> Tuple[figure, LayoutDOM]:
    """
    Plot a Process Tree Visualization.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing one or more Process Trees
    schema : ProcSchema, optional
        The data schema to use for the data set, by default None
        (if None the schema is inferred)
    output_var : str, optional
        Output variable for selected items in the tree,
        by default None
    legend_col : str, optional
        The column used to color the tree items, by default None
    show_table: bool
        Set to True to show a data table, by default False.

    Other Parameters
    ----------------
    height : int, optional
        The height of the plot figure
        (the default is 700)
    width : int, optional
        The width of the plot figure (the default is 900)
    title : str, optional
        Title to display (the default is None)
    hide_legend : bool, optional
        Hide the legend box, even if legend_col is specified.
    pid_fmt : str, optional
        Display Process ID as 'dec' (decimal) or 'hex' (hexadecimal),
        default is 'hex'.

    Returns
    -------
    Tuple[figure, LayoutDOM]:
        figure - The main bokeh.plotting.figure
        Layout - Bokeh layout structure.

    Raises
    ------
    ProcessTreeSchemaException
        If the data set schema is not valid for the plot.

    Notes
    -----
    The `output_var` variable will be overwritten with any selected
    values.

    """
    check_kwargs(kwargs, _DEFAULT_KWARGS)
    # reset_output()
    # output_notebook()

    plot_height: int = kwargs.pop("height", 700)
    plot_width: int = kwargs.pop("width", 900)
    title: str = kwargs.pop("title", "ProcessTree")
    hide_legend = kwargs.pop("hide_legend", False)
    pid_fmt = kwargs.pop("pid_fmt", "hex")

    proc_data, schema, levels, n_rows = _pre_process_tree(data, schema, pid_fmt=pid_fmt)
    if schema is None:
        raise ProcessTreeSchemaException("Could not infer data schema from data set.")

    source = ColumnDataSource(data=proc_data)
    # Get legend/color bar map
    fill_map, color_bar = _create_fill_map(source, legend_col)

    max_level = max(levels) + 3
    min_level = min(levels)

    if color_bar:
        title += " (color bar = {legend_col})"
    visible_range = int(plot_height / 35)
    y_start_range = (n_rows - visible_range, n_rows + 1)
    b_plot = figure(
        title=title,
        plot_width=int(plot_width) - 15,
        plot_height=plot_height,
        x_range=(min_level, max_level),
        y_range=y_start_range,
        tools=["reset", "save", "tap", "ywheel_pan"],
        toolbar_location="above",
        active_scroll="ywheel_pan",
    )

    hover = HoverTool(
        tooltips=_get_tool_tips(schema),
        formatters={f"@{schema.time_stamp}": "datetime"},
    )
    b_plot.add_tools(hover)

    # dodge to align rectangle with grid
    rect_x = dodge("Level", 1.75, range=b_plot.x_range)
    rect_plot_params = dict(
        width=3.5, height=0.95, source=source, fill_alpha=0.4, fill_color=fill_map
    )

    if color_bar:
        b_plot.add_layout(color_bar, "right")
    elif legend_col:
        rect_plot_params["legend_field"] = legend_col
    rect_plot = b_plot.rect(x=rect_x, y="Row", **rect_plot_params)
    if legend_col and not color_bar:
        b_plot.legend.title = legend_col
        b_plot.legend.label_text_font_size = "7pt"
    if hide_legend:
        b_plot.legend.visible = False

    text_props = {"source": source, "text_align": "left", "text_baseline": "middle"}

    def x_dodge(x_offset):
        return dodge("Level", x_offset, range=b_plot.x_range)

    def y_dodge(y_offset):
        return dodge("Row", y_offset, range=b_plot.y_range)

    b_plot.text(
        x=x_dodge(0.1),
        y=y_dodge(-0.2),
        text="__cmd_line$$",
        text_font_size="7pt",
        **text_props,
    )
    b_plot.text(
        x=x_dodge(0.1),
        y=y_dodge(0.25),
        text="__proc_name$$",
        text_font_size="8pt",
        **text_props,
    )
    b_plot.text(
        x=x_dodge(2.2),
        y=y_dodge(0.25),
        text="__proc_id$$",
        text_font_size="8pt",
        **text_props,
    )

    # Plot options
    _set_plot_option_defaults(b_plot)
    b_plot.xaxis.ticker = sorted(levels)
    b_plot.xgrid.ticker = sorted(levels)
    b_plot.hover.renderers = [rect_plot]  # only hover element boxes

    # Selection callback
    if output_var is not None:
        get_selected = _create_js_callback(source, output_var)
        b_plot.js_on_event("tap", get_selected)
        box_select = BoxSelectTool(callback=get_selected)
        b_plot.add_tools(box_select)

    range_tool = _create_vert_range_tool(
        data=source,
        min_y=0,
        max_y=n_rows,
        plot_range=b_plot.y_range,
        width=90,
        height=plot_height,
        x_col="Level",
        y_col="Row",
        fill_map=fill_map,
    )
    plot_elems = row(b_plot, range_tool)
    data_table = _create_data_table(source, schema, legend_col, width = int(plot_width) - 110, height = plot_height)
    plot_elems2 = row(plot_elems, data_table)
    # show(plot_elems)
    return plot_elems2


