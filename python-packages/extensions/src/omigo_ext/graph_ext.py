# package for plotting graph
import seaborn as sns
import matplotlib.pyplot as pyplot
import pandas as pd

from omigo_core import tsv
from omigo_core import utils

class VisualTSV(tsv.TSV):
    def __init__(self, header, data):
        super().__init__(header, data)

    def linechart(self, xcol, ycols, ylabel = None, title = None, subplots = False, xfigsize = 25, yfigsize = 5, props = None):
        return __pd_linechart__(self, xcol, ycols, ylabel, title, subplots, xfigsize, yfigsize, props)

    def scatterplot(self, xcol, ycol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10, props = None):
        return __sns_scatterplot__(self, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props)

    def histogram(self, xcol, class_col = None, bins = 10, title = None, binwidth = None, xfigsize = 25, yfigsize = 5, max_class_col = 10, props = None):
        return __sns_histogram__(self, xcol, class_col, bins, title, binwidth, xfigsize, yfigsize, max_class_col, props)

    def ecdf(self, xcol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_class_col = 10, props = None):
        return __sns_ecdf__(self, xcol, class_col, title, xfigsize, yfigsize, max_class_col, props)

    def density(self, ycols, class_col = None, title = None, xfigsize = 25, yfigsize = 5, props = None):
        return __sns_density__(self, ycols, class_col, title, xfigsize, yfigsize, props)

    def barchart(self, xcol, ycol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10, props = None):
        return __sns_barplot__(self, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props)

    def boxplot(self, xcol, ycol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10, props = None):
        return __sns_boxplot__(self, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props)

    def corr_heatmap(self, cols, title = None, xfigsize = 25, yfigsize = 5, max_rows = 6, props = None):
        return __sns_corr_heatmp__(self, cols, title, xfigsize, yfigsize, max_rows, props)

    def pairplot(self, cols, class_col = None, title = None, xfigsize = 5, yfigsize = 5, max_rows = 6, max_class_col = 6, props = None):
        return __sns_pairplot__(self, cols, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props)

def __create_data_frame_with_types__(xtsv, xcol = None, ycols = None, zcol = None):
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
        if (xtsv.__has_all_float_values__(col)):
            mp[col] = xtsv.col_as_float_array(col)
        else:
            mp[col] = xtsv.col_as_array(col)

    # zcols are returned without any extra transformation
    if (zcol is not None):
        mp[zcol] = xtsv.col_as_array(zcol)

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

def __pd_linechart__(xtsv, xcol, ycols, ylabel, title, subplots, xfigsize, yfigsize, props):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # validate ycols
    ycols = xtsv.__get_matching_cols__(ycols)

    # ylabel
    if (len(ycols) == 1 and ylabel is None):
        ylabel = ycols[0]

    # title
    if (title is None):
        title = ylabel

    # sort based on xcol
    xtsv = xtsv.sort(xcol)

    # create dataframe
    df = __create_data_frame_with_types__(xtsv, xcol, ycols, None)

    # plot
    df.plot.line(subplots = subplots, x = xcol, ylabel = ylabel, figsize = (xfigsize, yfigsize), title = title, **props2)

    # return
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

def __sns_scatterplot__(xtsv, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (max_rows < xtsv.num_rows()):
        if (utils.is_float_col(xtsv, xcol) == False or utils.is_float_col(xtsv, ycol) == False):
            utils.warn("Scatter plot on non numeric column(s). Doing downsampling for clean display to max_rows: {}".format(max_rows))
            xtsv = xtsv.sample_column_by_max_uniq_values(xcol, max_rows)

    # sort the columns based on their data types
    xtsv = xtsv.sort(xcol)

    # get dataframe
    df = __create_data_frame_with_types__(xtsv, xcol, [ycol], class_col)

    # plot
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # title
    if (title is None):
        title = "{} vs {}".format(xcol, ycol)

    # take hue order
    hue_order = sorted(xtsv.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # plot
    ax.set_title(title)
    plt = sns.scatterplot(ax = ax, x = xcol, y = ycol, hue = class_col, hue_order = hue_order, data = df, **props)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

def __sns_histogram__(xtsv, xcol, class_col, bins, title, binwidth, xfigsize, yfigsize, max_class_col, props):
    # default props
    default_props = dict(multiple = "dodge", shrink = 0.8, kde = False)
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # take class col if defined
    if (class_col is not None):
        xtsv = xtsv.sort([class_col, xcol])
    else:
        xtsv = xtsv.sort(xcol)

    # create dataframe
    df = __create_data_frame_with_types__(xtsv, xcol, None, class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xtsv.col_as_array_uniq(class_col)) if (class_col is not None) else None

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
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

def __sns_ecdf__(xtsv, xcol, class_col, title, xfigsize, yfigsize, max_class_col, props):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # take class col if defined
    if (class_col is not None):
        xtsv = xtsv.sort([class_col, xcol])
    else:
        xtsv = xtsv.sort(xcol)

    # create dataframe
    df = __create_data_frame_with_types__(xtsv, xcol, None, class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xtsv.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # binwidth overrides bins. TODO: This hue parameter is not giving class color consistently
    plt = sns.ecdfplot(data = df, x = xcol, hue = class_col, hue_order = hue_order, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

# the syntax is non intuitive. need to follow row major or column major. splitting by class_col is not possible
def __sns_density__(xtsv, ycols, class_col, title, xfigsize, yfigsize, props):
    # default props
    default_props = dict(multiple = "layer")
    props2 = __merge_props__(props, default_props)

    # create df
    ycols = xtsv.__get_matching_cols__(ycols)
    df = __create_data_frame_with_types__(xtsv, None, ycols, class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xtsv.col_as_array_uniq(class_col)) if (class_col is not None) else None

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
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

def __sns_barplot__(xtsv, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (len(xtsv.col_as_array_uniq(xcol)) > max_rows):
        utils.warn("Number of categorical values on x axis is too high: {}. Doing downsampling for clean display to max_rows: {}".format(len(xtsv.col_as_array_uniq(xcol)), max_rows))
        xtsv = xtsv.sample_column_by_max_uniq_values(xcol, max_rows)

    # take class col if defined
    if (class_col is not None):
        xtsv = xtsv.sort([class_col, xcol])
    else:
        xtsv = xtsv.sort(xcol)

    # create df
    df = __create_data_frame_with_types__(xtsv, xcol, ycol, class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xtsv.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # plot
    plt = sns.barplot(data = df, x = xcol, y = ycol, hue = class_col, hue_order = hue_order, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

def __sns_boxplot__(xtsv, xcol, ycol, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props):
    # default props
    default_props = dict()
    props2 = __merge_props__(props, default_props)

    # check number of unique class values
    if (class_col is not None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (len(xtsv.col_as_array_uniq(xcol)) > max_rows):
        utils.warn("Number of categorical values on x axis is too high: {}. Doing downsampling for clean display to max_rows: {}".format(len(xtsv.col_as_array_uniq(xcol)), max_rows))
        xtsv = xtsv.sample_column_by_max_uniq_values(xcol, max_rows)

    # sort the xcol
    xtsv = xtsv.sort(xcol)

    # create df
    df = __create_data_frame_with_types__(xtsv, xcol, ycol, class_col)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # take hue order
    hue_order = sorted(xtsv.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # plot 
    plt = sns.boxplot(data = df, x = xcol, y = ycol, hue = class_col, hue_order = hue_order, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

def __sns_corr_heatmp__(xtsv, cols, title, xfigsize, yfigsize, max_rows, props):
    # default props
    default_props = dict(annot = True)
    props2 = __merge_props__(props, default_props)

    # get matching cols
    cols = xtsv.__get_matching_cols__(cols)

    # validation for number of columns. if the number of unique values is too high, then raise exception
    if (len(cols) > max_rows):
        raise Exception("Number of columns is too high: {}. Max allowed: {}. Try max_rows".format(len(cols), max_rows))

    # check on the data type. Correlation is defined only on numerical columns
    for col in cols:
        if (utils.is_float_col(xtsv, col) == False):
            raise Exception("Non numeric column found for correlation: {}".format(col))

    # create df
    df = __create_data_frame_with_types__(xtsv, None, cols, None)

    # create figure
    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    # plot
    plt = sns.heatmap(df.corr(), **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

def __sns_pairplot__(xtsv, cols, class_col, title, xfigsize, yfigsize, max_rows, max_class_col, props):
    # default props
    default_props = dict(kind = None, diag_kind = None)
    props2 = __merge_props__(props, default_props)

    # find matching cols
    cols = xtsv.__get_matching_cols__(cols)

    # check number of unique class values
    if (class_col is not None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}. Max allowed: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # validation for number of columns. if the number of unique values is too high, then raise exception
    if (len(cols) > max_rows):
        raise Exception("Number of columns is too high: {}. Max allowed: {}. Try max_rows".format(len(cols), max_rows))

    # check on the data type. Correlation is defined only on numerical columns
    for col in cols:
        if (utils.is_float_col(xtsv, col) == False):
            raise Exception("Non numeric column found for correlation: {}".format(col))

    # create df
    df = __create_data_frame_with_types__(xtsv, None, cols, class_col)

    # take hue order
    hue_order = sorted(xtsv.col_as_array_uniq(class_col)) if (class_col is not None) else None

    # define aspect and plot
    aspect = xfigsize / yfigsize
    plt = sns.pairplot(df, hue = class_col, hue_order = hue_order, kind = kind, diag_kind = diag_kind, aspect = aspect, height = yfigsize, **props2)

    # check for title
    if (title is not None and title != ""):
        plt.set(title = title)

    # return
    return VisualTSV(xtsv.get_header(), xtsv.get_data())

