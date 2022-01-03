from omigo_core import tsv
from omigo_core import utils 
import random
import time
from kafka import KafkaConsumer

# some information that might help
# pip uninstall snappy python-snappy
# pip install python-snappy
# https://github.com/dask/fastparquet/issues/459

# Work In Progress
class KafkaClient:
    # constructor. group_id is created uniquely if not specified.
    def __init__(self, topic, bootstrap_servers, group_id, auto_offset_reset = "latest", value_deserializer = None, excluded_cols = None, url_encoded_cols = None,
        nested_cols = None):

        # check for value_deserializer
        if (value_deserializer is None):
            value_deserializer = lambda x: x.decode()

        # create consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers = bootstrap_servers,
            group_id = group_id,
            auto_offset_reset = auto_offset_reset,
            value_deserializer = value_deserializer
        )

        # json parsing need some tuning parameters
        self.excluded_cols = excluded_cols
        self.url_encoded_cols = url_encoded_cols
        self.nested_cols = nested_cols

        utils.warn("This KafkaConsumer is Work in Progress.")

    # method to read n messages from the topic.
    def read(self, n = 0, max_duration_sec = 0, sampling_rate = 1.0, seed = 0):
        # validtion. either n or max_duration_sec must be non zero
        if (n <= 0 and max_duration_sec <= 0):
            raise Exception("Either n or max_duration_sec must be non zero")
 
        # initialize random number generator
        random.seed(seed) #nosec

        # initialize start time. this will need better implementation to prevent infinite waiting. TODO
        ts_start = time.time()
        
        # iterate
        internal_prefix = "__KafkaClient_read__"
        new_header = internal_prefix
        new_data = []
        for message in self.consumer:
            # apply sampling
            if (sampling_rate > 1 or random.random() <= sampling_rate):  # nosec
                new_data.append(utils.url_encode(message.value))
            
            # check if all messages have been received, or time interval has been reached
            if (n > 0 and len(new_data) >= n):
                break

            # check if max_duration is exceeded
            ts_end = time.time()
            if (max_duration_sec > 0 and int(ts_end - ts_start) >= max_duration_sec):
                break

        # convert json to tsv
        return tsv.TSV(new_header, new_data) \
            .explode_json(internal_prefix, internal_prefix, excluded_cols = self.excluded_cols, url_encoded_cols = self.url_encoded_cols, nested_cols = self.nested_cols) \
            .remove_prefix(internal_prefix)
