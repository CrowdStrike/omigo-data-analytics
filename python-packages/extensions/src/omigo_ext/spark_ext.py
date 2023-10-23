from omigo_core import tsv, utils, funclib
from omigo_ext import sql_helper
import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

class SparkClient(sql_helper.HadoopSqlBase):
    def __init__(self, props = None, master = "yarn", app_name = "test"):
        # TODO: This needs to be changed
        import findspark
        if ("SPARK_HOME" in os.environ.keys()):
            findspark.init()

        # create spark session
        spark_conf = SparkConf() \
            .setMaster(master) \
            .setAppName(app_name) \
            .set("spark.sql.hive.caseSensitiveInferenceMode", "NEVER_INFER") \
            .set("spark.dynamicAllocation.enabled", "false") \
            .set("spark.yarn.maxAppAttempts", "1") \
            .set("spark.driver.maxResultSize", "4G") \
            .set("spark.executor.memoryOverhead", "1G") \
            .set("spark.executor.instances", "10") \
            .set("spark.executor.cores","4") \
            .set("spark.sql.shuffle.partitions", "100") \
            .set("spark.submit.deployMode", "client")

        # set parameters
        if (props is not None):
            for k in props.keys():
                spark_conf = spark_conf.set(k, props[k])

        # instantiate sql context
        spark_context = SparkContext.getOrCreate(conf = spark_conf)

        # instantiate spark
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.sql_context = SQLContext(spark_context)

    # Override
    def execute_query_in_engine(self, query):
        # Execute the query
        df = self.sql_context.sql(query)

        # convert to tsv
        xtsv = tsv.from_df(df.toPandas())

        # split the columns and data
        new_data = []
        for line in xtsv.get_data():
            new_data.append(line.split("\t"))

        # call stop
        self.__stop__()

        # return
        return xtsv.get_columns(), new_data

    # stop
    def __stop__(self):
        utils.info("SparkClient: stop called")
        self.spark.stop()
        self.spark = None
        self.spark_context = None
        self.sql_context = None

