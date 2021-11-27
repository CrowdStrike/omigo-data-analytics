# package for doing multi threading on specific apis

from concurrent.futures import ThreadPoolExecutor
from tsv_data_analytics import tsv
from tsv_data_analytics import tsvutils
from tsv_data_analytics import utils

import math
import time

class MultiThreadTSV(tsv.TSV):
    def __init__(self, header, data, num_par = 1, status_check_interval_sec = 10, sleep_interval_sec = 0.01):
        super().__init__(header, data)
        self.num_par = num_par
        self.status_check_interval_sec = status_check_interval_sec
        self.sleep_interval_sec = sleep_interval_sec

    def parallelize(self, func, *args, **kwargs):
        # split the data into num_par partitions
        batch_size = int(math.ceil(self.num_rows() / self.num_par))
        future_results = []

        # take start_time
        ts_start = time.time()

        # check for single threaded
        if (self.num_par == 1):
            combined_result = __parallelize__(self, func, *args, **kwargs)
        else:
            # run thread pool
            with ThreadPoolExecutor(max_workers = self.num_par) as executor:
                # execute batches concurrently based on num_par and batch_size 
                for i in range(self.num_par):
                    batch_i = self.skip(batch_size * i).take(batch_size)
                    future_results.append(executor.submit(__parallelize__, batch_i, func, *args, **kwargs))

                # run while loop
                while True:
                    done_count = 0
                    for f in future_results:
                        if (f.done() == True):
                            done_count = done_count + 1

                    # check if all are done
                    if (done_count < len(future_results)):
                        # sleep for some additional time to allow notebook stop method to work
                        utils.debug("transform_par: futures not completed yet. Sleeping for {} seconds".format(self.status_check_interval_sec))
                        time.sleep(self.status_check_interval_sec)
                    else:
                        break 
 
            # combine the results
            results = []
            for f in future_results:
                results.append(f.result())

            combined_result = tsvutils.merge(results)

        # take end_time 
        ts_end = time.time()

        utils.debug("parallelize: time taken: {} sec".format((ts_end - ts_start)))
        return combined_result

    def transform_par(self, *args, **kwargs):
        return self.parallelize(tsv.TSV.transform, *args, **kwargs)

def __parallelize__(xtsv, func, *args, **kwargs):
    return xtsv.custom_func(func, *args, **kwargs)

