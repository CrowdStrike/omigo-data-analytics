# extension for reading data from mysql
from omigo_core import tsv
import mysql.connector
import numpy as np

# some useful links for package installation
# https://stackoverflow.com/questions/50557234/authentication-plugin-caching-sha2-password-is-not-supported
# install mysql-connector-python and not mysql-connector
class MySQLClient:
    def __init__(self, host, username, password, database):
        self.client = mysql.connector.connect(
            host = host,
            user = username,
            password = password,
            database = database
        )

    # internal method to run a query
    def __query__(self, query_str):
        # fetch the cursory10
        cursor = self.client.cursor()
 
        # execute the query
        cursor.execute(query_str)

        # get the results
        return cursor.fetchall()

    # internal method to get the list of cols in case select * query is used
    def __get_cols__(self, table):
        # create query
        results = self.__query__("describe {}".format(table))

        # get col names
        cols = []
        for result in results:
            cols.append(result[0])

        # return
        return cols

    def __get_tables__(self):
        results = self.__get_tables__("show tables")

        # get table names
        table_names = []
        for result in results:
            table_names.append(result[0])

        # return
        return table_names

    # method to call select
    def select(self, table, cols = None, where_clause = None, limit = None, offset = None):
        # validation on column names
        table_cols = self.__get_cols__(table)
        table_names = self.__get_tables__()

        # check table name
        if (table not in table_names):
            raise Exception("Table not found in the database: {}, {}".format(table, table_names))

        # check column names
        if (cols is not None):
            for c in cols:
                if (c not in table_cols):
                    raise Exception("Column not found in the table: {}. all columns: {}".format(c, table_cols))

        # create basic query
        cols_str = "*" if (cols is None) else ",".join(["{}".format(c) for c in cols])
        query_str = "select {} from {}".format(cols_str, table)

        # add where clause
        if (where_clause is not None):
            query_str = "{} where {}".format(query_str, where_clause)

        # add limit
        if (limit is not None):
            query_str = "{} limit {}".format(query_str, limit)

        # add offset for pagination
        if (offset is not None):
           query_str = "{} offset {}".format(query_str, offset)

        # output cols
        output_cols = cols if (cols is not None) else table_cols

        # execute the query
        results = self.__query__(query_str)

        # create header
        header = "\t".join(output_cols)
        data = []

        # iterate
        for result in results:
            result_arr = np.asarray(result)
            result_str_arr = [str(t) for t in result_arr]
            data.append("\t".join(result_str_arr))

        # return tsv
        return tsv.TSV(header, data)
                
