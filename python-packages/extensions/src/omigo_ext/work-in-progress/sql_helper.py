from omigo_core import tsv, utils
import json

class HadoopSqlBase:
    def __init__(self):
        pass

    def execute_query(self, columns = ["*"], table = "__table_name__", distinct_flag = False, where_clause = "", group_by_cols = None, having_clause = "",
        order_by_cols = None, sort_order = None, limit = None, url_encoded_cols = None, map_cols = None):

        # some validation
        if (table is None):
            raise Exception("HadoopSqlBase: execute_query: table can not be none")

        # check for columns. TODO: read the default set of columns from table
        if ("*" in columns):
            utils.warn_once("HadoopSqlBase: execute_query: select * is not fully supported")

        effective_columns = list([col for col in columns]) 
        # for group by queries, add the group by columns to the select columns
        if (group_by_cols is not None):
            for col in group_by_cols:
                if (col not in columns):
                    effective_columns.append(col)

        # use empty arrays as defaults
        if (url_encoded_cols is None):
            url_encoded_cols = []
        if (map_cols is None):
            map_cols = []

        # do a lowercase on url_encoded_cols
        url_encoded_cols = list([v.lower() for v in url_encoded_cols])
        map_cols = list([v.lower() for v in map_cols]) 

        # base query
        if (distinct_flag == False):
            query = "select {} from {}".format(", ".join(effective_columns), table)
        else:
            query = "select distinct {} from {}".format(", ".join(effective_columns), table)

        # where clause
        if (where_clause != ""):
            query = "{} where {}".format(query, where_clause)

        # group by clause
        if (group_by_cols is not None):
            query = "{} group by {}".format(query, ", ".join(group_by_cols))

        # having clause
        if (having_clause != ""):
            query = "{} having {}".format(query, having_clause)

        # order by
        if (order_by_cols is not None):
            query = "{} order by {}".format(query, ", ".join(order_by_cols))

            # check sort order as asc or desc
            if (sort_order is not None):
                query = "{} {}".format(query, sort_order)

        # limit
        if (limit is not None):
            query = "{} limit {}".format(query, limit)

        # debug
        utils.info("HadoopSqlBase: execute_query: {}".format(query))

        # execute query
        result_cols, result_rows = self.execute_query_in_engine(query)

        # create the column aliases
        output_cols = []
        url_encoded_cols_indexes = {}
        map_cols_indexes = {}
 
        # iterate
        for i in range(len(result_cols)):
            # read column name
            c = result_cols[i]

            # check for url encoding
            if (c.lower() in url_encoded_cols):
                c = "{}:url_encoded".format(c)
                url_encoded_cols_indexes[i] = 1

            # check for map cols
            if (c.lower() in map_cols):
                c = "{}:json_encoded".format(c)
                map_cols_indexes[i] = 1

            # add column
            output_cols.append(c)

        # debug
        utils.info("HadoopSqlBase: execute_query: num rows: {}".format(len(result_rows)))

        # create data
        data = []
        for row in result_rows:
            # create result
            result_row = []

            # row is an array of values
            for i in range(len(row)):
                # read value
                value = row[i]

                # replace None with empty string
                if (value is None):
                    value = ""

                # replace any Ctrl-M characters
                value = "{}".format(value).replace("\t", " ").replace("\v", " ").replace("\r", " ").replace("\n", " ")

                # check if value needs to be url encoded
                if (i in url_encoded_cols_indexes.keys()):
                    value = utils.url_encode(value)

                # check if value needs to be converted to serialized json
                if (i in map_cols_indexes.keys()):
                    value = utils.url_encode(json.dumps(value))

                # append to result
                result_row.append(value)

            # TODO: there can be tabs in the data that can affect serialization
            cols_str = ["{}".format(t) for t in result_row]
            data.append("\t".join(cols_str))

        # create tsv. Do a validation as this is an external source
        xtsv = tsv.new_with_cols(output_cols, data = data).validate()

        # return
        return xtsv
        
    def execute_query_in_engine(self, query):
        raise Exception("HadoopSqlBase: execute_query: derived class must implement this method")    

