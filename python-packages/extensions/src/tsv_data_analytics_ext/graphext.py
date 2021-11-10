# package for plotting graph
import seaborn as sns
import matplotlib.pyplot as pyplot
import numpy as np
import pandas as pd
import math

from tsv_data_analytics import tsv
from tsv_data_analytics import utils 


def __create_data_frame_with_types__(xtsv, xcol = None, ycols = None, zcol = None):
    # convert to array
    if (ycols != None):
        if (utils.is_array_of_string_values(ycols) == False):
            ycols = [ycols]
            
    # merge the two columns
    combined_cols = []

    # xcol
    if (xcol != None):
        combined_cols.append(xcol)
        
    # ycols
    if (ycols != None):
        for col in ycols:
            combined_cols.append(col)
           
    # create map for data frame
    mp = {}
    for col in combined_cols:
        if (tsv.is_float_col(xtsv, col)):
            mp[col] = xtsv.col_as_float_array(col)
        else:
            mp[col] = xtsv.col_as_array(col)
            
    # zcols are returned without any extra transformation
    if (zcol != None):
        mp[zcol] = xtsv.col_as_array(zcol)
    
    return pd.DataFrame(mp)
        
def linechart(xtsv, xcol, ycols, ylabel = None, subplots = False, xfigsize = 25, yfigsize = 5):
    # validate ycols
    ycols = xtsv.__get_matching_cols__(ycols)
    
    # ylabel
    if (len(ycols) == 1 and ylabel == None):
        ylabel = ycols[0]

    # sort based on xcol
    xtsv = xtsv.sort(xcol)

    # create dataframe
    df = __create_data_frame_with_types__(xtsv, xcol, ycols)

    # plot
    df.plot.line(subplots = subplots, x = xcol, ylabel = ylabel, figsize = (xfigsize, yfigsize))
    
def scatterplot(xtsv, xcol, ycol, class_col = None, title = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10):
    # check number of unique class values    
    if (class_col != None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))
        
    # if xcol or ycol are non numeric then need to down sample the data
    if (max_rows < xtsv.num_rows()):
        if (tsv.is_float_col(xtsv, xcol) == False or tsv.is_float_col(xtsv, ycol) == False):
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
    if (title == None):
        title = "{} vs {}".format(xcol, ycol)
        
    #df.plot.scatter(x = xcol, y = ycol, figsize = figsize, title = title)    
    ax.set_title(title)
    sns.scatterplot(ax = ax, x = xcol, y = ycol, hue = class_col, data = df)
    
def histogram(xtsv, xcol, class_col = None, bins = 10, title = None, binwidth = None, kde = False, multiple = "dodge", xfigsize = 25, yfigsize = 5, max_class_col = 10):
    # check number of unique class values
    if (class_col != None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))
    
    df = __create_data_frame_with_types__(xtsv, xcol, None, class_col)

    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)
    
    # binwidth overrides bins
    if (binwidth != None):
        sns.histplot(data = df, x = xcol, hue = class_col, binwidth = binwidth, kde = kde, multiple = multiple, shrink = 0.8)
    else:
        sns.histplot(data = df, x = xcol, hue = class_col, bins = bins, kde = kde, multiple = multiple, shrink = 0.8)
        
def density(xtsv, ycols, xfigsize = 25, yfigsize = 5):
    # create df
    df = __create_data_frame_with_types__(xtsv, ycols = ycols)

    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    sns.kdeplot(data = df)
        
def barchart(xtsv, xcol, ycol, class_col = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10):
    # check number of unique class values
    if (class_col != None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (len(xtsv.col_as_array_uniq(xcol)) > max_rows):
        utils.warn("Number of categorical values on x axis is too high. Doing downsampling for clean display to max_rows: {}".format(max_rows))
        xtsv = xtsv.sample_column_by_max_uniq_values(xcol, max_rows)

    # sort the xcol
    xtsv = xtsv.sort(xcol)
    
    # create df
    df = __create_data_frame_with_types__(xtsv, xcol, ycol, class_col)

    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    sns.barplot(data = df, x = xcol, y = ycol, hue = class_col)

def boxplot(xtsv, xcol, ycol, class_col = None, xfigsize = 25, yfigsize = 5, max_rows = 20, max_class_col = 10):
    # check number of unique class values
    if (class_col != None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))

    # if xcol or ycol are non numeric then need to down sample the data
    if (len(xtsv.col_as_array_uniq(xcol)) > max_rows):
        utils.warn("Number of categorical values on x axis is too high. Doing downsampling for clean display to max_rows: {}".format(max_rows))
        xtsv = xtsv.sample_column_by_max_uniq_values(xcol, max_rows)

    # sort the xcol
    xtsv = xtsv.sort(xcol)
    
    # create df
    df = __create_data_frame_with_types__(xtsv, xcol, ycol, class_col)

    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    sns.boxplot(data = df, x = xcol, y = ycol, hue = class_col)
    
def corr_heatmap(xtsv, cols, xfigsize = 25, yfigsize = 5, max_rows = 6):
    # validation for number of columns. if the number of unique values is too high, then raise exception
    if (len(cols) > max_rows):
        raise Exception("Number of columns is too high: {}. Max allowed: {}. Try max_rows".format(len(cols), max_rows))
        
    # check on the data type. Correlation is defined only on numerical columns
    for col in cols:
        if (tsv.is_float_col(xtsv, col) == False):
            raise Exception("Non numeric column found for correlation: {}".format(col))

    # create df
    df = __create_data_frame_with_types__(xtsv, None, cols, None)

    figsize = (xfigsize, yfigsize)
    fig, ax = pyplot.subplots(figsize = figsize)

    sns.heatmap(df.corr(), annot = True)
    
def pairplot(xtsv, cols, class_col = None, kind = None, diag_kind = None, xfigsize = 5, yfigsize = 5, max_rows = 6, max_class_col = 6):
    # check number of unique class values
    if (class_col != None and len(xtsv.col_as_array_uniq(class_col)) >= max_class_col):
        raise Exception("Number of class column values is more than {}. Max allowed: {}. Probably not a class column. Try max_class_col".format(max_class_col, len(xtsv.col_as_array_uniq(class_col))))
    
    # validation for number of columns. if the number of unique values is too high, then raise exception
    if (len(cols) > max_rows):
        raise Exception("Number of columns is too high: {}. Max allowed: {}. Try max_rows".format(len(cols), max_rows))
        
    # check on the data type. Correlation is defined only on numerical columns
    for col in cols:
        if (tsv.is_float_col(xtsv, col) == False):
            raise Exception("Non numeric column found for correlation: {}".format(col))

    # create df
    df = __create_data_frame_with_types__(xtsv, None, cols, class_col)

    aspect = xfigsize / yfigsize
    sns.pairplot(df, hue = class_col, kind = kind, diag_kind = diag_kind, aspect = aspect, height = yfigsize)

