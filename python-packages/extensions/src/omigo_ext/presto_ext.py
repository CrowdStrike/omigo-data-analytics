import prestodb
import os
from omigo_core import tsv
from omigo_core import utils
from omigo_ext import sql_helper

# TODO: this needs to be moved to an inner class. the public class needs to return a TSV object
class PrestoClient(sql_helper.HadoopSqlBase):
    def __init__(self, host = "127.0.0.1", port = 8889, user = None, catalog = "hive", schema = "default"):
        if (host is None):
            utils.warn("PrestoClient: host is null. Using localhost")
            host = "127.0.0.1"

        # check user
        if (user is None):
            user = os.environ["USER"]

        # initialize connection
        self.conn = prestodb.dbapi.connect(
            host = host,
            port = port,
            user = user,
            catalog = catalog,
            schema = schema
        )

    # Override
    def __execute_query_in_engine__(self, query):
        # execute query
        cur = self.conn.cursor()
        result = cur.execute(query)
        rows = cur.fetchall()

        # get the list of columns
        cols = list([desc[0] for desc in cur.description]) 

        # return tuple 
        return cols, rows

    def execute_query_in_engine(self, query):
        attempts_remaining = 3
        exc = None
        while (attempts_remaining > 0):
            try:
                return self.__execute_query_in_engine__(query)
            except Exception as e:
                utils.warn("execute_query_in_engine: exception caught for query: {}, error: {}, attempts_remaining: {}".format(query, e, attempts_remaining))
                attempts_remaining = attempts_remaining - 1
                exc = e

        # declare failure
        raise exc
