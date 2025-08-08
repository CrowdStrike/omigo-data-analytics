from omigo_core import dataframe, utils
import json

class HadoopSqlBase:
    def __init__(self):
        pass

    def execute_query(self, columns = ["*"], table = None, where_clause = "", group_by_cols = None, having_clause = "",
        order_by_cols = None, sort_order = None, distinct_flag = None, limit = None, map_cols = None):

        # warn
        utils.warn_once("HadoopSqlBase: execute_query: this is provided as reference implementation and should be used carefully in prod to avoid issues with sql")

        # some validation
        if (table is None):
            raise Exception("HadoopSqlBase: execute_query: table can not be none")

        # check for columns. TODO: read the default set of columns from table
        if ("*" in columns):
            utils.warn_once("HadoopSqlBase: execute_query: select * is not fully supported")

        # create effective_columns
        effective_columns = []

        # for group by queries, add the group by columns to the select columns
        if (group_by_cols is not None):
            for gcol in group_by_cols:
                found = False
                for col in columns:
                    col = col.lower()
                    index = col.find(" as ")
                    # check for aliases
                    if (index != -1):
                        col = col[0:index].strip()

                    # check for match
                    if (col == gcol.lower()):
                        found = True
                        break

                # if col is not found, add to effective_columns
                if (found == False):
                    effective_columns.append(gcol)

        # add select colummns after group by
        for col in columns:
            effective_columns.append(col)

        # use empty arrays as defaults
        if (map_cols is None):
            map_cols = []

        # do a lowercase on mapcols
        map_cols = list([v.lower() for v in map_cols])

        # base query
        if (distinct_flag is None or distinct_flag == False):
            query = "select {} from {}".format(", ".join(effective_columns), table) # nosec
        else:
            query = "select distinct {} from {}".format(", ".join(effective_columns), table) # nosec

        # where clause
        if (where_clause != ""):
            query = "{} where {}".format(query, where_clause)

        # group by clause
        if (group_by_cols is not None and len(group_by_cols) > 0):
            group_by_cols_expr = ",".join(["{}".format(i+1) for i in range(len(group_by_cols))])
            query = "{} group by {}".format(query, group_by_cols_expr)

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
        map_cols_indexes = {}

        # iterate
        for i in range(len(result_cols)):
            # read column name
            c = result_cols[i]

            # check for map cols
            if (c.lower() in map_cols):
                map_cols_indexes[i] = 1

            # add column
            output_cols.append(c)

        # debug
        utils.info("HadoopSqlBase: execute_query: num rows: {}".format(len(result_rows)))

        # create data
        data_fields = []
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

                # check if value needs to be converted to serialized json
                if (i in map_cols_indexes.keys()):
                    value = json.dumps(value)

                # replace any Ctrl-M characters
                # value = "{}".format(value).replace("\t", " ").replace("\v", " ").replace("\r", " ").replace("\n", " ")

                # append to result
                result_row.append(value)

            # TODO: there can be tabs in the data that can affect serialization
            # cols_str = ["{}".format(t) for t in result_row]
            data_fields.append(result_row)

        # create tsv. Do a validation as this is an external source
        xdf = dataframe \
            .new_with_cols(output_cols, data_fields = data_fields) \
            .validate()

        # return
        return xdf

    def execute_query_in_engine(self, query):
        raise Exception("HadoopSqlBase: execute_query: derived class must implement this method")

