# package for doing multi threading on specific apis

from concurrent.futures import ThreadPoolExecutor
from omigo_core import tsv
from omigo_core import tsvutils
from omigo_core import utils

import math
import time

class MultiThreadTSV(tsv.TSV):
    def __init__(self, header, data, num_par = 0, status_check_interval_sec = 10, sleep_interval_sec = 0.11, num_batches = 10, inherit_message = ""):
        super().__init__(header, data)
        self.num_par = num_par
        self.status_check_interval_sec = status_check_interval_sec
        self.sleep_interval_sec = sleep_interval_sec
        self.inherit_message = inherit_message + ": MultiThreadTSV" if (inherit_message != "") else "MultiThreadTSV"

        # check if num_par is more than number of rows
        if (self.num_rows() < self.num_par):
            utils.warn("{}: num_rows: {} < num_par: {}. Adjusting the value".format(self.inherit_message, self.num_rows(), self.num_par))
            self.num_par = self.num_rows()

        # set the num_batches for better splitting
        self.num_batches = num_batches if (num_batches > num_par) else num_par

    def parallelize(self, func, *args, **kwargs):
        # trace
        utils.trace("{}: parallelize: func: {}, args: {}, kwargs: {}".format(self.inherit_message, func, *args, **kwargs))

        # split the data into num_par partitions
        batch_size = int(math.ceil(self.num_rows() / self.num_batches))
        future_results = []

        # take start_time
        ts_start = time.time()

        # check for single threaded
        if (self.num_par == 0):
            utils.info("{}: running in single threaded mode.".format(self.inherit_message))
            combined_result = __parallelize__(self, func, *args, **kwargs)
        else:
            # print batch size
            utils.info("{}: num_rows: {}, num_par: {}, num_batches: {}, batch_size: {}, status_check_interval_sec: {}".format(self.inherit_message, 
                self.num_rows(), self.num_par, self.num_batches, batch_size, self.status_check_interval_sec))

            # run thread pool
            with ThreadPoolExecutor(max_workers = self.num_par) as executor:
                # execute batches concurrently based on num_par and batch_size 
                for i in range(self.num_batches):
                    batch_i = self.skip(batch_size * i).take(batch_size)
                    # TODO: rewrite this logic. Right now dont submit empty batches
                    if (batch_i.num_rows() > 0):
                        future_results.append(executor.submit(__parallelize__, batch_i, func, *args, **kwargs))

                # track total waiting time
                time_elapsed = 0
                # run while loop
                while True:
                    done_count = 0
                    for f in future_results:
                        if (f.done() == True):
                            done_count = done_count + 1

                    # debug
                    utils.debug("{}: parallelize: done_count: {}, total: {}".format(self.inherit_message, done_count, len(future_results)))

                    # check if all are done
                    if (done_count < len(future_results)):
                        # sleep for some additional time to allow notebook stop method to work
                        utils.debug("{}: parallelize: futures not completed yet. Sleeping for {} sec. Time elapsed: {} sec".format(self.inherit_message, self.status_check_interval_sec, time_elapsed))
                        time.sleep(self.status_check_interval_sec)
                        time_elapsed = time_elapsed + self.status_check_interval_sec
                    else:
                        break 
 
            # combine the results
            results = []
            for f in future_results:
                results.append(f.result())

            # merge the tsvs using a common union.
            combined_result = tsvutils.merge(results, def_val_map = {})

        # take end_time 
        ts_end = time.time()

        utils.debug("{}: parallelize: time taken: {} sec, num_rows: {}".format(self.inherit_message, int(ts_end - ts_start), combined_result.num_rows()))
        return combined_result

def __parallelize__(xtsv, func, *args, **kwargs):
    return func(xtsv, *args, **kwargs)

