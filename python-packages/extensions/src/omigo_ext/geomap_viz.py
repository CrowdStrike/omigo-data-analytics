from omigo_core import tsv, utils

import pandas as pd
from bokeh.io import output_notebook, show
from bokeh.plotting import figure, ColumnDataSource
from bokeh.palettes import *
from bokeh.transform import linear_cmap
from bokeh.models import ColorBar, NumeralTickFormatter, MultiLine, Div, Circle
from bokeh.models import ColumnDataSource

# Define class
class GeoMapTSV(tsv.TSV):
    def __init__(self, header, data):
        super().__init__(header, data)

    # helper method to map lat long to mercators
    def __x_coord__(self, x, y):
        lat = x
        lon = y

        r_major = 6378137.000
        x = r_major * np.radians(lon)
        scale = x / lon
        y = 180.0 / np.pi * np.log(np.tan(np.pi / 4.0 + lat * (np.pi / 180.0) / 2.0)) * scale
        return (x, y)  

    # TODO: this api can change to abstract the bokeh library
    def geomap_plot(self, lat_col, lon_col, display_cols_mp = {}, width = 1200, height = 430, use_fixed_layout = True, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "GeoMapTSV: geomap_plot")

        # prefix
        prefix = "__geomap__"

        # extend display_cols_mp
        display_cols_mp = display_cols_mp.copy()
        display_cols_mp[lat_col] = "Latitude"
        display_cols_mp[lon_col] = "Longitude"

        # map ip to latitude and longitude
        xtsv = self \
            .select(list(display_cols_mp.keys()), dmsg = dmsg) \
            .is_nonempty_str(lat_col) \
            .is_nonempty_str(lon_col) \
            .is_nonzero_float(lat_col) \
            .is_nonzero_float(lon_col) \
            .distinct(dmsg = dmsg) \
            .add_const("circle_size", "15") \
            .add_const("color_index", "0") \
            .add_const("circle_alpha", "0.8")

        # if any of the columns contain colon(:), then map them with underscore as bokeh tooltips use colon as special character
        ks = list(display_cols_mp.keys())
        for k in ks:
             if (k.find(":") != -1):
                 k2 = k.replace(":", "_")
                 xtsv = xtsv.rename(k, k2)
                 display_cols_mp[k2] = display_cols_mp[k]
                 del display_cols_mp[k]

        # update lat-lon cols
        lat_col = lat_col.replace(":", "_")
        lon_col = lon_col.replace(":", "_")

        # create data frame
        df = xtsv.to_df(infer_data_types = True)

        # map the lat-long to mercators
        df["{}:coordinates".format(prefix)] = list(zip(df[lat_col], df[lon_col]))
        mercators = [self.__x_coord__(x, y) for x, y in df["{}:coordinates".format(prefix)] ]
        df["{}:mercator".format(prefix)] = mercators
        df[["{}:mercator_x".format(prefix), "{}:mercator_y".format(prefix)]] = df["{}:mercator".format(prefix)].apply(pd.Series)

        # Create figure with boundaries of the entire world
        min_x, max_x, min_y, max_y = -13624971.673499351, 16832321.97793506, -4011071.4166808245, 6895498.946934601

        # use constant layout if required
        if (use_fixed_layout == False):
            # boundaries of the world map
            min_x = min(df["{}:mercator_x".format(prefix)])
            max_x = max(df["{}:mercator_x".format(prefix)])
            min_y = min(df["{}:mercator_y".format(prefix)])
            max_y = max(df["{}:mercator_y".format(prefix)])
        
        # debug
        # utils.debug("min_x: {}, max_x: {}, min_y: {}, max_y: {}".format(min_x, max_x, min_y, max_y))

        # Choose palette
        color_index_max = 10
        palette = Category20[color_index_max]
        
        # Define color mapper - which column will define the colour of the data points
        low = 0
        high = color_index_max 
        color_mapper = linear_cmap(field_name = 'color_index', palette = palette, low = low, high = high)
        
        # Set tooltips - these appear when we hover over a data point in our map, very nifty and very useful
        nan_color = '#d9d9d9'
        tooltips = list([(display_cols_mp[t], "@" + t) for t in display_cols_mp.keys()])
        
        # adjust
        min_x = 0.5 * min_x if (min_x >= 0) else 1.5 * min_x
        max_x = 1.5 * max_x if (max_x >= 0) else 0.5 * max_x
        min_y = 0.5 * min_y if (min_y >= 0) else 1.5 * min_y
        max_y = 1.5 * max_y if (max_y >= 0) else 0.5 * max_y
        
        # create figure
        p = figure(x_axis_type = "mercator", y_axis_type = "mercator",  x_axis_label = 'Longitude', y_axis_label = 'Latitude', tooltips = tooltips,
            width = width, height = height, x_range = [min_x, max_x], y_range = [min_y, max_y])
        
        # Add map tile
        p.add_tile("CartoDB Positron", retina = True)

        # xs and ys 
        df["xs"] = df["{}:mercator_x".format(prefix)]
        df["ys"] = df["{}:mercator_y".format(prefix)]
        
        # Add points using mercator coordinates
        c1 = Circle(x = "{}:mercator_x".format(prefix), y = "{}:mercator_y".format(prefix), fill_color = color_mapper, size = "circle_size", fill_alpha = "circle_alpha")
        m1 = MultiLine(xs = "xs", ys = "ys", line_width = 1, line_dash = "dotted", line_alpha = "circle_alpha")

        # define the columns in data source
        cols = ["{}:mercator_x".format(prefix), "{}:mercator_y".format(prefix), "xs", "ys", "circle_size", "circle_alpha", "color_index"] + list(display_cols_mp.keys())

        # create map for columns and assign data values
        mp = {}
        for c in cols:
            mp[c] = df[c]

        # create column source 
        sources = {"default": ColumnDataSource(mp)}
        p.add_glyph(sources["default"], m1)
        p.add_glyph(sources["default"], c1)
        
        # Defines color bar
        color_bar = ColorBar(color_mapper=color_mapper['transform'], formatter = NumeralTickFormatter(format='0.0[0000]'), label_standoff = 13, width = 8, location = (0,0))
        
        # Set color_bar location
        p.add_layout(color_bar, 'right')

        # use directive to output to notebook 
        output_notebook()
        show(p)
       
        # return
        return self 
