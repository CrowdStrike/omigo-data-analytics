from omigo_core import utils 
from omigo_core import tsv
from omigo_core import tsvutils
from omigo_core import funclib 
from omigo_ext import multithread_ext
import datetime
from dateutil import parser
import splunklib.client as splunk_client
import splunklib.results as splunk_results
import os
import json
import time


# set splunk host
SPLUNK_HOST = os.environ["SPLUNK_HOST"] if ("SPLUNK_HOST" in os.environ.keys()) else ""

# class to search splunk
class SplunkSearch:
    def __init__(self, host = SPLUNK_HOST, timeout_sec = 300, wait_sec = 5, attempts = 3, enable_cache = False, use_partial_results = False):
        # initialize parameters
        self.host = host
        self.timeout_sec = timeout_sec
        self.wait_sec = wait_sec
        self.attempts = attempts
        self.enable_cache = enable_cache
        self.use_partial_results = use_partial_results

        # initialize parameters
        self.filters = []
        self.cols = []
        self.start_time = None

        # use cache
        self.cache = {} if (self.enable_cache == True) else None

        # check for presence of credentials
        if ("SPLUNK_USER" not in os.environ.keys() or "SPLUNK_PASS" not in os.environ.keys()):
            raise Exception("SPLUNK_USER or SPLUNK_PASS not defined as environment variables")
    
        # create splunk_service once
        self.splunk_service = splunk_client.connect(host = self.host, username = os.environ["SPLUNK_USER"], password = os.environ["SPLUNK_PASS"])

        # message at creating a new instance
        utils.info("SplunkSearch: new instance created: host: {}, timeout_sec: {}, wait_sec: {}, attempts: {}, enable_cache: {}, use_partial_results: {}".format(self.host, self.timeout_sec, self.wait_sec,
            self.attempts, self.enable_cache, self.use_partial_results))

    def set_max_results(self, m):
        self.max_results = m
        return self

    def add_filters(self, filter_tuples):
        for key, value in filter_tuples:
            self.filters.append((key, value))
        return self

    def add_filter(self, key, value):
        self.filters.append((key, value))
        return self

    def select_cols(self, cols = None):
        self.cols = cols
        return self

    def set_time_range(self, start_time, end_time = None):
        # end time is optional
        if (end_time == None):
            end_time = "now"

        # resolve time strings
        self.start_time = self.__resolve_time_str__(start_time)
        self.end_time = self.__resolve_time_str__(end_time)

        # return
        return self
    
    def __get_filter_query__(self):
        # validation
        if (self.start_time == None or self.end_time == None):
            raise Exception("Missing mandatory parameters")

        # filters is mandatory
        if (len(self.filters) == 0):
            raise Exception("Missing mandatory parameters")

        # check max results
        if (self.max_results == None):
            raise Exception("max_results is mandatory for adding some safe guards")

        # construct query
        query = "search " + " and ".join(['{}="{}"'.format(x, y) for x,y in self.filters])

        # add select
        if (self.cols != None and len(self.cols) > 0):
            query = "{} | table {}".format(query, select_str)

        # add max results
        query = "{} | head {}".format(query, self.max_results)

        utils.debug("SplunkSearch: __get_filter_query__: query: {}".format(query))
        # return
        return query

    def simple_filter_query(self):
        # construct query and run job
        query = self.__get_filter_query__()
        return self.__execute_query__(query, self.start_time, self.end_time, self.cols, self.attempts)

    def complex_query(self, query, start_time, end_time = None, cols = None):
        # warn
        utils.warn_once("SplunkSearch: complex_query() api is provided for research purposes only. Please use with caution")

        # set default
        if (end_time == None):
            end_time = "now"

        # resolve time (probably again)
        start_time = self.__resolve_time_str__(start_time)
        end_time = self.__resolve_time_str__(end_time)

        # execute
        return self.__execute_query__(query, start_time, end_time, cols, self.attempts, exec_mode = "normal")

    # inner method for calling query 
    def __execute_query__(self, query, start_time, end_time, cols, attempts_remaining, exec_mode = "normal"):
        if (exec_mode == "normal"):
            return self.__execute_normal_query__(query, start_time, end_time, cols, attempts_remaining)
        else:
            return self.__execute_blocking_query__(query, start_time, end_time, cols, attempts_remaining)

    def __execute_normal_query__(self, query, start_time, end_time, cols, attempts_remaining):
        # set default parameters
        search_kwargs = {
            "earliest_time": start_time,
            "latest_time": end_time,
            "exec_mode": "normal"
        }

        # debug 
        utils.info("query: {}, attempts_remaining: {}, search_kwargs: {}".format(query, attempts_remaining, str(search_kwargs)))

        # check cache
        cache_key = "{}:{}".format(query, str(search_kwargs))
        if (self.enable_cache == True and cache_key in self.cache.keys()):
            utils.debug_once("SplunkSearch: __execute_query__: returning result from cache: {}".format(cache_key))
            return self.cache[cache_key]

        # execute query
        try: 
            splunk_job = self.splunk_service.jobs.create(query, **search_kwargs)
            exec_start_time = funclib.get_utctimestamp_sec() 
            # A normal search returns the job's SID right away, so we need to poll for completion
            while True:
                # check for job to be ready. not sure what this does, just following example
                # https://dev.splunk.com/enterprise/docs/devtools/python/sdk-python/howtousesplunkpython/howtorunsearchespython/
                while not splunk_job.is_ready():
                    # check for timeout
                    exec_cur_time = funclib.get_utctimestamp_sec()
                    if (exec_cur_time - exec_start_time > self.timeout_sec):
                        raise Exception("__execute_normal_query__: timeout reached, failed to finish query")

                    # else pass
                    utils.debug("__execute_normal_query__: waiting for is_ready, sleeping for {} seconds".format(self.wait_sec))
                    time.sleep(self.wait_sec)
                
                # check stats
                stats = {
                    "isDone": splunk_job["isDone"],
                    "doneProgress": float("%.2f" % (float(splunk_job["doneProgress"]) * 100)),
                    "scanCount": int(splunk_job["scanCount"]),
                    "eventCount": int(splunk_job["eventCount"]),
                    "resultCount": int(splunk_job["resultCount"])
                }

                # debug
                utils.info("__execute_normal_query__: progress stats: {}".format(stats))

                # check if job is done
                if (stats["isDone"] == "1"):
                    utils.info("__execute_normal_query__: done: {}".format(stats))
                    break

                # check the current time    
                exec_cur_time = funclib.get_utctimestamp_sec()

                # check for timeout
                if (exec_cur_time - exec_start_time > self.timeout_sec):
                    # try if partial results are available
                    if (self.use_partial_results == True and stats["resultCount"] > 0):
                        utils.warn("__execute_normal_query__: timeout reached. use_partial_results is enabled. num results: {}".format(stats["resultCount"]))
                        break
                    else:
                        raise Exception("__execute_normal_query__: timeout reached, failed to finish query")

                # sleep
                utils.debug("__execute_normal_query__: waiting for {} seconds".format(self.wait_sec))
                time.sleep(self.wait_sec)

            # get the results
            result = self.__parse_results__(splunk_job, query, start_time, end_time, cols)

            # cache the results if needed
            if (self.enable_cache == True):
                self.cache[cache_key] = result

            # cancel the job
            splunk_job.cancel()

            # return
            return result
        except Exception as e:
            # check if multiple attempts are needed
            if (attempts_remaining > 0):
                # debug
                utils.info("SplunkSearch: __execute_normal_query__: caught exception: {}, attempts remaining: {}".format(str(e), attempts_remaining))

                # call again with lesser attempts_remaining
                return self.__execute_normal_query__(query, start_time, end_time, cols, attempts_remaining - 1)
            else:
                utils.error("Exception: {}".format(str(e)))
                base_mp = self.__create_empty_results_map__(query, start_time, end_time)
                base_mp["__count__"] = "0"
                base_mp["__error_msg__"] = str(e)
                result = tsvutils.load_from_array_of_map([base_mp])

                # cache if needed
                if (self.enable_cache == True):
                    self.cache[cache_key] = result

                # cancel the job
                splunk_job.cancel()

                # return
                return result
    
    def __execute_blocking_query__(self, query, start_time, end_time, cols, attempts_remaining):
        # set default parameters
        search_kwargs = {
            "earliest_time": start_time,
            "latest_time": end_time,
            "exec_mode": "blocking"
        }

        # debug 
        utils.info("query: {}, attempts_remaining: {}, search_kwargs: {}".format(query, attempts_remaining, str(search_kwargs)))

        # check cache
        cache_key = "{}:{}".format(query, str(search_kwargs))
        if (self.enable_cache == True and cache_key in self.cache.keys()):
            utils.debug_once("SplunkSearch: __execute_blocking_query__: returning result from cache: {}".format(cache_key))
            return self.cache[cache_key]

        # execute query
        try: 
            splunk_job = self.splunk_service.jobs.create(query, **search_kwargs)
            result = self.__parse_results__(splunk_job, query, start_time, end_time, cols)
            if (self.enable_cache == True):
                self.cache[cache_key] = result
            # cancel the job
            splunk_job.cancel()
            return result
        except Exception as e:
            # check if multiple attempts are needed
            if (attempts_remaining > 0):
                # debug
                utils.info("SplunkSearch: __execute_blocking_query__: caught exception: {}, attempts remaining: {}".format(str(e), attempts_remaining))

                # call again with lesser attempts_remaining
                return self.__execute_blocking_query__(query, start_time, end_time, cols, attempts_remaining - 1)
            else:
                # error
                utils.error(str(e))
                base_mp = self.__create_empty_results_map__(query, start_time, end_time)
                base_mp["__count__"] = "0"
                base_mp["__error_msg__"] = str(e)
                result = tsvutils.load_from_array_of_map([base_mp])
                if (self.enable_cache == True):
                    self.cache[cache_key] = result
                # cancel the job
                splunk_job.cancel()
                return result
 
    def __create_empty_results_map__(self, query, start_time, end_time):
        # create base map
        return {"__start_time__": start_time, "__end_time__": end_time, "__error_msg__": "", "__count__": "" }

    # splunk returns lot of things. One of them is tag::eventtype which is excluded
    def __parse_results__(self, splunk_job, query, start_time, end_time, cols):
        # create base map
        base_mp = self.__create_empty_results_map__(query, start_time, end_time)

        # check for empty results
        results_total = int(splunk_job["resultCount"])
        base_mp["__count__"] = str(results_total)
        base_mp["__error_msg__"] = ""
        utils.debug("SplunkSearch: query: {}, results_total: {}".format(query, results_total))

        # check for empty results
        if (results_total == 0):
            # return base tsv
            return tsvutils.load_from_array_of_map([base_mp])

        # define iterator variables
        results_offset = 0
        result_page_size = 100

        # variable to store results
        results = []
        while results_offset < results_total:
            # append the result map to results
            reader = splunk_results.JSONResultsReader(splunk_job.results(count = result_page_size, offset = results_offset, output_mode="json"))
            # utils.info("reader.is_preview: {}".format(reader))
            for result in reader:
                # check if it is a dict obect with results or some warning/error message
                if (isinstance(result, (dict))):
                    # keys starting with underscore _ seem to be internal to splunk and not useful here.
                    filtered_result = {}
                    for k in result.keys():
                        if (k.startswith("_") == False and k.find("::") == -1):
                            if (isinstance(result[k], list)):
                                filtered_result[k] = ",".join(list(["{}".format(t) for t in result[k]]))
                            elif (isinstance(result[k], dict)):
                                utils.warn_once("SplunkSearch: key with dictionary as result type found. This is not fully supported: {}".format(k))
                                filtered_result[k] = json.dumps(result[k])
                            else:
                                filtered_result[k] = str(result[k])

                    # append filtered_result
                    results.append(filtered_result)
                    # utils.debug("__parse_results__: result: {}".format(result.keys()))
                else:
                    result_str = str(result)
                    if (result_str.find("WARN") == -1):
                        raise Exception("result of non dict type found: {}".format(result_str))
                    else:
                        warn_msg = result_str if (len(result_str) <= 10) else result_str[0:10]
                        utils.info_once("Found a warning message: {}".format(warn_msg))

            # increment offset
            results_offset = results_offset + result_page_size

        # combine base results
        for result in results:
            for k in base_mp.keys():
                result[k] = str(base_mp[k])

        # apply default cs level url encoding
        results2 = []
        for result in results:
            # create a new map
            result2 = {}

            # assign key-values with modified name
            for k in result:
                # take original value
                key2 = k
                value2 = result[k]

                # check if the key-values are to be url encoded
                if (key2 in TextContentCols):
                    key2 = "{}:url_encoded".format(key2)
                    value2 = utils.url_encode(value2)

                # assign to new map
                result2[key2] = str(value2).replace("\t", " ").replace("\v", " ").replace("\r", " ").replace("\n", " ")

            # append to array
            results2.append(result2)

        # construct tsv from the list of hashmaps
        return tsvutils.load_from_array_of_map(results2)
        
    def __resolve_time_str__(self, x):
        # check for specific syntax with now
        x = x.replace(" ", "")
        if (x.startswith("now")):
            base_time = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)
            diff_sec = None 
            # check if there are any diff units
            if (x == "now"):
                diff_sec = 0
            else:
                # validation for parsing logic
                if (x.startswith("now-") == False):
                    raise Exception("Unknown operator against now: ", x)

                # take the diff part
                diffstr = x[len("now-"):]

                # unit is single letter, 'd', 'h', 'm', 's'
                unit = diffstr[-1]

                # how many of units to apply
                count = int(diffstr[0:-1])
                if (unit == "d"):
                    diff_sec = count * 86400
                elif (unit == "h"):
                    diff_sec = count * 3600
                elif (unit == "m"):
                    diff_sec = count * 60
                elif (unit == "s"):
                    diff_sec = count * 1
                else:
                    raise Exception("Unknown time unit:", parts[1])

            # return base_time minus diff
            # return datetime.datetime.utcfromtimestamp(int(base_time.timestamp()) - diff_sec).replace(tzinfo = datetime.timezone.utc).isoformat()
            return funclib.utctimestamp_to_datetime_str(int(base_time.timestamp()) - diff_sec)
        else:
            return funclib.utctimestamp_to_datetime_str(funclib.datetime_to_utctimestamp(x))

# class to do data manipulation on TSV
class SplunkTSV(tsv.TSV):
    def __init__(self, header, data, splunk_search = None, host = SPLUNK_HOST, timeout_sec = 300, wait_sec = 5, attempts = 3, enable_cache = False, use_partial_results = False, num_par = 0, max_results = 10, inherit_message = ""):
        MAX_NUM_PAR = 4
        super().__init__(header, data)
        self.splunk_search = SplunkSearch(host = host, timeout_sec = timeout_sec, wait_sec = wait_sec, attempts = attempts, enable_cache = enable_cache, use_partial_results = use_partial_results) if (splunk_search is None) else splunk_search
        self.host = host
        self.timeout_sec = timeout_sec
        self.wait_sec = wait_sec
        self.attempts = attempts
        self.enable_cache = enable_cache
        self.use_partial_results = use_partial_results
        self.num_par = num_par
        self.max_results = max_results
        self.inherit_message = inherit_message + ": SplunkTSV" if (inherit_message != "") else "SplunkTSV"

        # do a hard cap on max number of threads to be allowed
        if (self.num_par > MAX_NUM_PAR):
            utils.warn("SplunkTSV: num_par requested: {}, max_allowed: {}.".format(num_par, MAX_NUM_PAR))
            self.num_par = MAX_NUM_PAR

    #def get_events_par(self, query_filter, start_ts_col, end_ts_col, prefix, inherit_message = ""):
    def get_events_par(self, *args, **kwargs):
        return self \
            .extend_class(multithread_ext.MultiThreadTSV, num_par = self.num_par) \
                .parallelize(__multithreaded_get_events__, self.splunk_search, self.inherit_message, *args, **kwargs)


