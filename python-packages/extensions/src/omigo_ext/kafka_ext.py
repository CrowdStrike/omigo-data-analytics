from omigo_core import tsv
from omigo_core import utils 
import random
import time
from kafka import KafkaConsumer

# some information that might help
# pip uninstall snappy python-snappy
# pip install python-snappy
# https://github.com/dask/fastparquet/issues/459
class KafkaClient:
    # constructor. group_id is created uniquely if not specified.
    def __init__(self, topic, bootstrap_servers, group_id = None, auto_offset_reset = "latest", value_deserializer = None):
        # create a new group_id
        if (group_id is None):
            group_id = "{}:{}:{}".format(topic, bootstrap_servers, time.time())

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

    # method to read n messages from the topic.
    def read(self, prefix, n = 0, max_duration_sec = 0, sampling_rate = 1.0, seed = 0):
        # validtion. either n or max_duration_sec must be non zero
        if (n <= 0 and max_duration_sec <= 0):
            raise Exception("Either n or max_duration_sec must be non zero")
 
        # initialize random number generator
        random.seed(seed) #nosec

        # initialize start time. this will need better implementation to prevent infinite waiting. TODO
        ts_start = time.time()
        
        # iterate
        new_header = prefix + ":json"
        new_data = []
        for message in self.consumer:
            # apply sampling
            if (sampling_rate > 1 or random.random() <= sampling_rate):  # nosec
                new_data.append(utils.url_encode(message.value))
            
            # check if all messages have been received, or time interval has been reached
            ts_end = time.time()
            if (n > 0 and len(new_data) >= n):
                break

            if (max_duration_sec > 0 and int(ts_end - ts_start) >= max_duration_sec):
                break

        # convert json to tsv
        return tsv.TSV(new_header, new_data) \
            .explode_json(prefix + ":json", prefix)
