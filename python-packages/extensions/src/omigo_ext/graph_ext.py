# package for plotting graph
import seaborn as sns
import matplotlib.pyplot as pyplot
import pandas as pd

from omigo_core import dataframe
from omigo_core import utils
from omigo_core import udfs 

class VisualDF(dataframe.DataFrame):
    def __init__(self, header_fields, data_fields):
        super().__init__(header_fields, data_fields)

    def linechart(self, xcol, ycols, ylabel = None, title = None, subplots = False, xfigsize = 25, yfigsize = 5, props = {}, dmsg = ""):
        return __pd_linechart__(self, xcol, ycols, ylabel, title, subplots, xfigsize, yfigsize, props, dmsg)

    def linechart_multi_class(self, xcol, ycol, class_col, ylabel = None, title = None, subplots = False, xfigsize = 25, yfigsize = 5, props = {}, dmsg = ""):
        return __pd_linechart_multi_class__(self, xcol, ycol, class_col, ylabel, title, subplots, xfigsize, yfigsize, props, dmsg)

    def scatterplot(self, xcol, ycol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10, props = {}, dmsg = ""):
        return __sns_scatterplot__(self, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg)

    def histogram(self, xcol, class_col = None, bins = 10, title = None, binwidth = None, xfigsize = 25, yfigsize = 5, max_class_col = 10, props = {}, dmsg = ""):
        return __sns_histogram__(self, xcol, class_col, bins, title, binwidth, xfigsize, yfigsize, max_class_col, props, dmsg)

    def ecdf(self, xcol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_class_col = 10, props = {}, dmsg = ""):
        return __sns_ecdf__(self, xcol, class_col, title, xfigsize, yfigsize, max_class_col, props, dmsg)

    def density(self, ycols, class_col = None, title = None, xfigsize = 25, yfigsize = 5, props = {}, dmsg = ""):
        return __sns_density__(self, ycols, class_col, title, xfigsize, yfigsize, props, dmsg)

    def barchart(self, xcol, ycol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10, props = {}, dmsg = ""):
        return __sns_barplot__(self, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg)

    def boxplot(self, xcol, ycol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10, props = {}, dmsg = ""):
        return __sns_boxplot__(self, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg)

    def corr_heatmap(self, cols, title = None, xfigsize = 25, yfigsize = 5, max_rows = 6, props = {}, dmsg = ""):
        return __sns_corr_heatmp__(self, cols, title, xfigsize, yfigsize, max_rows, props, dmsg)

    def pairplot(self, cols, class_col = None, title = None, xfigsize = 5, yfigsize = 5, max_rows = 6, max_class_col = 6, props = {}, dmsg = ""):
        return __sns_pairplot__(self, cols, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg)

def __create_data_frame_with_types__(xdf, xcol = None, ycols = None, zcol = None):
    # convert to array
    if (ycols is not None):
        if (utils.is_array_of_string_values(ycols) == False):
            ycols = [ycols]

    # merge the two columns
    combined_cols = []

    # xcol
    if (xcol is not None):
        combined_cols.append(xcol)

    # ycols
    if (ycols is not None):
        for col in ycols:
            combined_cols.append(col)

    # create map for data frame
    mp = {}
    for col in combined_cols:
        # TODO: change this logic. Using non public method
        if (xdf.__has_all_float_values__(col)):
            mp[col] = xdf.col_as_float_array(col)
        else:
            mp[col] = xdf.col_as_array(col)

    # zcols are returned without any extra transformation
    if (zcol is not None):
        mp[zcol] = xdf.col_as_array(zcol)

    return pd.DataFrame(mp)

def __merge_props__(props, default_props):
    # create new dict
    props2 = dict()

    # take new props
    if (props is not None):
        for k in props.keys():
            props2[k] = props[k]

    # take default if not defined
    if (default_props is not None):
        for k in default_props.keys():
            if (k not in props2.keys()):
                props2[k] = default_props[k]

    # return
    return props2

def __pd_linechart__(xdf, xcol, ycols, ylabel, title, subplots, xfigsize, yfigsize, props, dmsg = ""):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # validate ycols
    ycols = xdf.__get_matching_cols__(ycols)

    # ylabel
    if (len(ycols) == 1 and ylabel is None):
        ylabel = ycols[0]

    # title
    if (title is None):
        title = ylabel

    # sort based on xcol
    xdf = xdf.sort(xcol)

    # create dataframe
    df = __create_data_frame_with_types__(xdf, xcol = xcol, ycols = ycols, zcol = None)

    # plot
    df.plot.line(subplots = subplots, x = xcol, ylabel = ylabel, figsize = (xfigsize, yfigsize), title = title, **props2)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __pd_linechart_multi_class__(xdf, xcol, ycol, class_col, ylabel, title, subplots, xfigsize, yfigsize, props, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "__pd_linechart_multi_class__")

    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # validate ycol
    if (ycol is None):
        raise Exception("{}: ycol is None".format(dmsg))

    # validate class_col
    if (class_col is None):
        raise Exception("{}: class_col is None".format(dmsg))

    # ylabel
    if (ylabel is None):
        ylabel = ycol

    # title
    if (title is None):
        title = ylabel

    # select
    xdf = xdf.select([xcol, ycol, class_col])

    # check if there are duplicate rows
    if (xdf.num_rows() != xdf.select([xcol, class_col]).distinct().num_rows()):
        utils.warn("{}: Duplicate data found. Doing aggregation".format(dmsg))
        xdf = xdf \
            .aggregate([xcol, class_col], [ycol], [udfs.sumfloat], dmsg = dmsg) \
            .transform("{}:sumfloat".format(ycol), lambda t: int(float(t)), ycol) \
            .drop_cols("{}:sumfloat".format(ycol))

    # sort based on xcol
    xdf = xdf.sort(xcol)

    # split into multiple dataframes based on class
    class_values = xdf.col_as_array_uniq(class_col)
    xdf_classes = []
    xcol_df = xdf.select(xcol).add_const("__pd_linechart_multi_class_temp_col__", "")

    # iterate
    for i in range(len(class_values)):
        class_value = str(class_values[i])
        xdf_classes.append(xdf.eq_str(class_col, class_value))

    # merge
    xdf_merged = xcol_df
    for i in range(len(class_values)):
        xdf_merged = xdf_merged \
            .left_map_join(xdf_classes[i], [xcol], rsuffix = class_values[i], default_val = "0", dmsg = dmsg) \
            .rename("{}:{}".format(ycol, class_values[i]), class_values[i])

    # drop temp col
    xdf_merged = xdf_merged.drop_cols("__pd_linechart_multi_class_temp_col__")

    # create dataframe
    df = __create_data_frame_with_types__(xdf_merged, xcol = xcol, ycols = class_values, zcol = None)

    # plot
    df.plot.line(subplots = subplots, x = xcol, ylabel = ylabel, figsize = (xfigsize, yfigsize), title = title, **props2)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __sns_scatterplot__(xdf, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg = ""):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xdf.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xdf.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (max_rows < xdf.num_rows()):
        if (utils.is_float_col(xdf, xcol) == False or utils.is_float_col(xdf, ycol) == False):
            utils.warn("Scatter plot on non numeric column(s). Doing downsampling for clean display to max_rows: {}".format(max_rows))
            xdf = xdf.sample_column_by_max_uniq_values(xcol, max_rows)

    # sort the columns based on their data types
    xdf = xdf.sort(xcol)

    # get dataframe
    df = __create_data_frame_with_types__(xdf, xcol = xcol, ycols =[ycol], zcol = class_col)

    # plot
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # title
    if (title is None):
        title = "{} vs {}".format(xcol, ycol)

    # take hue order
    hue_order = sorted(xdf.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # plot
    ax.set_title(title)
    plt = sns.scatterplot(ax = ax, x = xcol, y = ycol, hue = class_col, hue_order = hue_order, data = df, **props)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __sns_histogram__(xdf, xcol, class_col, bins, title, binwidth, xfigsize, yfigsize, max_class_col, props, dmsg = ""):
    # default props
    default_props = dict(multiple = "dodge", shrink = 0.8, kde = False)
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xdf.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xdf.col_as_array_uniq(class_col))))

    # take class col if defined
    if (class_col is not None):
        xdf = xdf.sort([class_col, xcol])
    else:
        xdf = xdf.sort(xcol)

    # create dataframe
    df = __create_data_frame_with_types__(xdf, xcol = xcol, ycols = None, zcol = class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xdf.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # binwidth overrides bins. TODO: This hue parameter is not giving class color consistently
    plt = None
    if (binwidth is not None):
        plt = sns.histplot(data = df, x = xcol, hue = class_col, hue_order = hue_order, binwidth = binwidth, **props2)
    else:
        plt = sns.histplot(data = df, x = xcol, hue = class_col, hue_order = hue_order, bins = bins, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __sns_ecdf__(xdf, xcol, class_col, title, xfigsize, yfigsize, max_class_col, props, dmsg = ""):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xdf.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xdf.col_as_array_uniq(class_col))))

    # take class col if defined
    if (class_col is not None):
        xdf = xdf.sort([class_col, xcol])
    else:
        xdf = xdf.sort(xcol)

    # create dataframe
    df = __create_data_frame_with_types__(xdf, xcol = xcol, ycols = None, zcol = class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xdf.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # binwidth overrides bins. TODO: This hue parameter is not giving class color consistently
    plt = sns.ecdfplot(data = df, x = xcol, hue = class_col, hue_order = hue_order, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

# the syntax is non intuitive. need to follow row major or column major. splitting by class_col is not possible
def __sns_density__(xdf, ycols, class_col, title, xfigsize, yfigsize, props, dmsg = ""):
    # default props
    default_props = dict(multiple = "layer")
    props2 = __merge_props__(props, default_props)

    # create df
    ycols = xdf.__get_matching_cols__(ycols)
    df = __create_data_frame_with_types__(xdf, xcol = None, ycols = ycols, zcol = class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xdf.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # TODO: This is not clean
    # multiple = props["multiple"] if (props is not None and "multiple" in props.keys()) else "layer"

    # check for class col
    plt = None
    if (class_col is not None):
        if (len(ycols) == 1):
            plt = sns.kdeplot(data = df, x = ycols[0], hue = class_col, hue_order = hue_order, **props2)
        else:
            raise Exception("__sns_density__: class_col with multiple ycols is not supported: {}".format(ycols))
    else:
       plt = sns.kdeplot(data = df, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __sns_barplot__(xdf, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg = ""):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xdf.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xdf.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (len(xdf.col_as_array_uniq(xcol)) > max_rows):
        utils.warn("Number of categorical values on x axis is too high: {}. Doing downsampling for clean display to max_rows: {}".format(len(xdf.col_as_array_uniq(xcol)), max_rows))
        xdf = xdf.sample_column_by_max_uniq_values(xcol, max_rows)

    # take class col if defined
    if (class_col is not None):
        xdf = xdf.sort([class_col, xcol])
    else:
        xdf = xdf.sort(xcol)

    # create df
    df = __create_data_frame_with_types__(xdf, xcol = xcol, ycols = [ycol], zcol = class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xdf.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # plot
    plt = sns.barplot(data = df, x = xcol, y = ycol, hue = class_col, hue_order = hue_order, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __sns_boxplot__(xdf, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg = ""):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xdf.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xdf.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (len(xdf.col_as_array_uniq(xcol)) > max_rows):
        utils.warn("Number of categorical values on x axis is too high: {}. Doing downsampling for clean display to max_rows: {}".format(len(xdf.col_as_array_uniq(xcol)), max_rows))
        xdf = xdf.sample_column_by_max_uniq_values(xcol, max_rows)

    # sort the xcol
    xdf = xdf.sort(xcol)

    # create df
    df = __create_data_frame_with_types__(xdf, xcol = xcol, ycols = [ycol], zcol = class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xdf.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # plot
    plt = sns.boxplot(data = df, x = xcol, y = ycol, hue = class_col, hue_order = hue_order, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __sns_corr_heatmp__(xdf, cols, title, xfigsize, yfigsize, max_rows, props, dmsg = ""):
    # default props
    default_props = dict(annot = True)
    props2 = __merge_props__(props, default_props)

    # get matching cols
    cols = xdf.__get_matching_cols__(cols)

    # validation for number of columns. if the number of unique values is too high, then raise exception
    if (len(cols) > max_rows):
        raise Exception("Number of columns is too high: {}. Max allowed: {}. Try max_rows".format(len(cols), max_rows))

    # check on the data type. Correlation is defined only on numerical columns
    for col in cols:
        if (utils.is_float_col(xdf, col) == False):
            raise Exception("Non numeric column found for correlation: {}".format(col))

    # create df
    df = __create_data_frame_with_types__(xdf, xcol = None, ycols = cols, zcol = None)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # plot
    plt = sns.heatmap(df.corr(), **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

def __sns_pairplot__(xdf, cols, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props, dmsg = ""):
    # default props
    default_props = dict(kind = None, diag_kind = None)
    props2 = __merge_props__(props, default_props)

    # find matching cols
    cols = xdf.__get_matching_cols__(cols)

    # check number of unique class values
    if (class_col is not None and len(xdf.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}. Max allowed: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xdf.col_as_array_uniq(class_col))))

    # validation for number of columns. if the number of unique values is too high, then raise exception
    if (len(cols) > max_rows):
        raise Exception("Number of columns is too high: {}. Max allowed: {}. Try max_rows".format(len(cols), max_rows))

    # check on the data type. Correlation is defined only on numerical columns
    for col in cols:
        if (utils.is_float_col(xdf, col) == False):
            raise Exception("Non numeric column found for correlation: {}".format(col))

    # create df
    df = __create_data_frame_with_types__(xdf, xcol = None, ycols = cols, zcol = class_col)

    # take hue order
    hue_order = sorted(xdf.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # define aspect and plot
    aspect = xfigsize / yfigsize
    plt = sns.pairplot(df, hue = class_col, hue_order = hue_order, aspect = aspect, height = yfigsize, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualDF(xdf.get_header_fields(), xdf.get_data_fields())

