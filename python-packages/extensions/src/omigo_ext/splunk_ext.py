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
import math
import traceback

# NOTES: 
# 1. where is tricky. use search if syntax is not well tested
# 2. Push simple lookup filters in front even though if they sound redundant
# 3. there is no evidence so far that differentiates between using different steps with | or using just AND
# 4. There is a TERM constant for string constants match and avoids doing splitting on special characters
# class to search splunk
class SplunkSearch:
    def __init__(self, host, app = None, username = None, password = None, cookie = None, timeout_sec = 600, wait_sec = 10, attempts = 3, enable_cache = False, use_partial_results = False,
        cache_instance_timeout_sec = 1800, attempt_sleep_sec = 30):
        # Validation
        if (host is None):
            raise Exception("Missing parameters: host")

        # Validation
        if (app is None):
            raise Exception("TODO: Need app parameter. Not sure what happens if app is None")

        # check for cookie or username/password
        if (cookie is None):
            if (username is None):
                raise Exception("Missing parameters: username")

            if (password is None):
                raise Exception("Missing parameters: password")

        # use_partial_results is just a concept at this point
        if (use_partial_results is not None and use_partial_results == True):
            raise Exception("SplunkSearch: use_partial_results is not implemented/tested yet")

        # initialize parameters
        self.host = host
        self.app = app
        self.username = username
        self.password = password
        self.cookie = cookie
        self.timeout_sec = timeout_sec
        self.wait_sec = wait_sec
        self.attempts = attempts
        self.enable_cache = enable_cache
        self.use_partial_results = use_partial_results
        self.cache_instance_timeout_sec = cache_instance_timeout_sec
        self.attempt_sleep_sec = attempt_sleep_sec

        # initialize parameters
        self.filters = []
        self.cols = []
        self.start_time = None

        # use cache
        self.cache = {} if (self.enable_cache == True) else None

        # message at creating a new instance
        utils.info("SplunkSearch: new instance created: host: {}, app: {}, timeout_sec: {}, wait_sec: {}, attempts: {}, enable_cache: {}, use_partial_results: {}".format(
            self.host, self.app, self.timeout_sec, self.wait_sec, self.attempts, self.enable_cache, self.use_partial_results))

    def get_splunk_service(self):
        # create splunk_service once
        if (self.cookie is not None):
            return splunk_client.connect(host = self.host, cookie = self.cookie, app = self.app)
        else:
            return splunk_client.connect(host = self.host, username = self.username, password = self.password, app = self.app)

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

    def simple_filter_query(self, include_internal_fields = False):
        # construct query and run job
        query = self.__get_filter_query__()
        return self.__execute_query__(query, self.start_time, self.end_time, self.cols, self.attempts, include_internal_fields = include_internal_fields)

    def call_search(self, query, start_time, end_time = None, url_encoded_cols = None, include_internal_fields = False, limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "SplunkSearch: call_search")

        # warn
        utils.debug_once("SplunkSearch: call_search() api is provided for research purposes only. Please use with caution")

        # do some safety checks
        if (query.find("fields *") == -1):
            if (query.find("stats") == -1 and query.find("dedup") == -1):
                utils.warn_once("SplunkSearch: call_search(): for non aggregate queries, always use '| fields *' as the last operator otherwise splunk might return only limited columns: {}".format(query))

        # set default
        if (end_time == None):
            end_time = "now"

        # resolve time (probably again)
        start_time = self.__resolve_time_str__(start_time)
        end_time = self.__resolve_time_str__(end_time)

        # execute
        return self.__execute_query__(query, start_time, end_time, self.attempts, exec_mode = "normal", url_encoded_cols = url_encoded_cols,
            include_internal_fields = include_internal_fields, limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg)

    # get splunk job id for display
    def __get_splunk_job_display_id__(self, splunk_job):
        if (splunk_job is not None):
            return splunk_job["name"].split("-")[0]
        else:
            return ""

    # inner method for calling query 
    def __execute_query__(self, query, start_time, end_time, attempts_remaining, exec_mode = "normal", url_encoded_cols = None, include_internal_fields = False,
        limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "SplunkSearch: __execute_query__")

        # check for blocking or non-blocking execution
        if (exec_mode == "normal"):
            return self.__execute_normal_query__(query, start_time, end_time, url_encoded_cols, attempts_remaining, include_internal_fields, limit, num_par_on_limit, dmsg = dmsg)
        else:
            return self.__execute_blocking_query__(query, start_time, end_time, url_encoded_cols, attempts_remaining, include_internal_fields, limit, num_par_on_limit, dmsg = dmsg)

    def __split_time_slots__(self, st, et, num_splits):
        start_ts = funclib.datetime_to_utctimestamp(st)
        end_ts = funclib.datetime_to_utctimestamp(et)

        # find time width
        width = int(math.floor((end_ts - start_ts) / num_splits))

        # create slots
        slots = []
        for i in range(num_splits):
            st2 = funclib.utctimestamp_to_datetime_str(start_ts + i * width)
            if (i == num_splits - 1):
                et2 = funclib.utctimestamp_to_datetime_str(end_ts)
            else:
                et2 = funclib.utctimestamp_to_datetime_str(start_ts + (i + 1) * width)
            slots.append((st2, et2))

        # return
        return slots

    def __execute_normal_query__(self, query, start_time, end_time, url_encoded_cols, attempts_remaining, include_internal_fields, limit, num_par_on_limit, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "SplunkSearch: __execute_normal_query__")

        # set default parameters
        search_kwargs = {
            "earliest_time": start_time,
            "latest_time": end_time,
            "exec_mode": "normal"
        }

        # debug 
        utils.info("{}: query: {}, attempts_remaining: {}, search_kwargs: {}".format(utils.max_dmsg_str(dmsg), query, attempts_remaining, str(search_kwargs)))

        # check cache
        cache_key = "{}:{}".format(query, str(search_kwargs))
        if (self.enable_cache == True and cache_key in self.cache.keys()):
            utils.debug_once("{}: returning result from cache: {}".format(dmsg, cache_key))
            return self.cache[cache_key]

        # execute query
        try: 
            splunk_job = self.get_splunk_service().jobs.create(query, **search_kwargs)
            job_id_trim = self.__get_splunk_job_display_id__(splunk_job)
            utils.info("{}: Splunk Job submitted: {}".format(utils.max_dmsg_str(dmsg), job_id_trim))

            exec_start_time = funclib.get_utctimestamp_sec() 
            # A normal search returns the job's SID right away, so we need to poll for completion
            while True:
                # check for job to be ready. not sure what this does, just following example
                # https://dev.splunk.com/enterprise/docs/devtools/python/sdk-python/howtousesplunkpython/howtorunsearchespython/
                while not splunk_job.is_ready():
                    # check for timeout
                    exec_cur_time = funclib.get_utctimestamp_sec()
                    exec_diff_sec = exec_cur_time - exec_start_time
                    if (exec_diff_sec > self.timeout_sec):
                        utils.error_and_raise_exception("{}: timeout: {} > {} reached, failed to finish query".format(utils.max_dmsg_str(dmsg), exec_diff_sec, self.timeout_sec))

                    # else pass
                    utils.info("{}: waiting for is_ready, sleeping for {} seconds".format(utils.max_dmsg_str(dmsg), self.wait_sec))
                    time.sleep(self.wait_sec)
                
                # check stats
                stats = {
                    "isDone": splunk_job["isDone"],
                    "doneProgress": splunk_job["doneProgress"],
                    "scanCount": int(splunk_job["scanCount"]),
                    "eventCount": int(splunk_job["eventCount"]),
                    "resultCount": int(splunk_job["resultCount"])
                }

                # debug
                utils.info("Status: job_id: {}, {}".format(job_id_trim, stats))

                # check if job is done
                if (stats["isDone"] == "1"):
                    utils.info("Finished: Status: job_id: {}, {}".format(job_id_trim, stats))
                    break

                # check the current time    
                exec_cur_time = funclib.get_utctimestamp_sec()

                # check for timeout
                if (exec_cur_time - exec_start_time > self.timeout_sec):
                    # try if partial results are available
                    if (self.use_partial_results == True and stats["resultCount"] > 0):
                        utils.warn("{}: timeout reached. use_partial_results is enabled. num results: {}".format(utils.max_dmsg_str(dmsg), stats["resultCount"]))
                        break
                    else:
                        utils.error_and_raise_exception("{}: timeout reached, failed to finish query".format(utils.max_dmsg_str(dmsg)))

                # sleep
                utils.debug("{}: waiting for {} seconds".format(utils.max_dmsg_str(dmsg), self.wait_sec))
                time.sleep(self.wait_sec)

            # check if limit is defined
            result = None
            if (limit is not None and num_par_on_limit > 1):
                # need to check if we need to overcome limit parameter
                results_total = int(splunk_job["resultCount"])

                # compare with limit
                if (results_total >= limit):
                    if (num_par_on_limit > 1):
                        utils.warn("{}: limit: {} reached, splitting the query into {} parts and running again".format(dmsg, results_total, num_par_on_limit))
                        # dont make furthen than 1 level deep call
                        limit2 = None
                        num_par_on_limit2 = 0

                        # split the time range into num_par_on_limit slots
                        time_slots = self.__split_time_slots__(start_time, end_time, num_par_on_limit)
                        xtsv_results = []

                        # iterate
                        for (st, et) in time_slots:
                            xtsv_result = self.__execute_normal_query__(query, st, et, url_encoded_cols, attempts_remaining, include_internal_fields,
                                limit2, num_par_on_limit2, dmsg = dmsg)
                            utils.info("{}: Executed query with new time range: start_time: {}, end_time: {}, num_rows: {}".format(dmsg, st, et, xtsv_result.num_rows()))
                            # check if results are still exceeding limit
                            if (xtsv_result.num_rows() >= limit):
                                utils.warn("{}: Split results still exceeded the limit. The results will be partial: {}".format(dmsg, xtsv_result.num_rows()))
                            xtsv_results.append(xtsv_result)

                        # merge
                        result = tsv.merge_union(xtsv_results)
                    else:
                        utils.warn("{}: limit: {} reached, but num_par_on_limit = 0. Results would be partial".format(dmsg, results_total))

            # get the result if they were not returned by limit calls
            if (result is None):
                # get the results
                result = self.__parse_results__(splunk_job, query, start_time, end_time, url_encoded_cols, include_internal_fields)

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
                utils.warn("{}: caught exception: {}, attempts remaining: {}".format(utils.max_dmsg_str(dmsg), str(e), attempts_remaining))
                # utils.error("{}: Stack Trace: {}".format(utils.max_dmsg_str(dmsg), traceback.format_exc()))

                # call again with lesser attempts_remaining
                utils.info("{}: Sleeping for {} seconds before attempting again".format(dmsg, self.attempt_sleep_sec))
                time.sleep(self.attempt_sleep_sec)
                return self.__execute_normal_query__(query, start_time, end_time, url_encoded_cols, attempts_remaining - 1, include_internal_fields, limit,
                    num_par_on_limit, dmsg = dmsg)
            else:
                utils.error("{}: Exception: {}".format(utils.max_dmsg_str(dmsg), str(e)))
                base_mp = self.__create_empty_results_map__(query, start_time, end_time)
                base_mp["__count__"] = "0"
                base_mp["__error_msg__"] = str(e)
                result = tsv.from_maps([base_mp])

                # cache if needed
                if (self.enable_cache == True):
                    self.cache[cache_key] = result

                # cancel the job
                splunk_job.cancel()

                # return
                return result
    
    def __execute_blocking_query__(self, query, start_time, end_time, url_encoded_cols, attempts_remaining, include_internal_fields, limit, num_par_on_limit, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "SplunkSearch: __execute_blocking_query__")

        # set default parameters
        search_kwargs = {
            "earliest_time": start_time,
            "latest_time": end_time,
            "exec_mode": "blocking"
        }

        # debug 
        utils.info("{}: query: {}, attempts_remaining: {}, search_kwargs: {}".format(utils.max_dmsg_str(dmsg), query, attempts_remaining, str(search_kwargs)))

        # check cache
        cache_key = "{}:{}".format(query, str(search_kwargs))
        if (self.enable_cache == True and cache_key in self.cache.keys()):
            utils.debug_once("SplunkSearch: __execute_blocking_query__: returning result from cache: {}".format(cache_key))
            return self.cache[cache_key]

        # execute query
        try: 
            splunk_job = self.get_splunk_service().jobs.create(query, **search_kwargs)
            result = self.__parse_results__(splunk_job, query, start_time, end_time, url_encoded_cols, include_internal_fields)
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
                utils.info("{}: Sleeping for {} seconds before attempting again".format(dmsg, self.attempt_sleep_sec))
                time.sleep(self.attempt_sleep_sec)
                return self.__execute_blocking_query__(query, start_time, end_time, url_encoded_cols, attempts_remaining - 1, include_internal_fields, limit, num_par_on_limit)
            else:
                # error
                utils.error(str(e))
                base_mp = self.__create_empty_results_map__(query, start_time, end_time)
                base_mp["__count__"] = "0"
                base_mp["__error_msg__"] = str(e)
                result = tsv.from_maps([base_mp])
                if (self.enable_cache == True):
                    self.cache[cache_key] = result
                # cancel the job
                splunk_job.cancel()
                return result
 
    def __create_empty_results_map__(self, query, start_time, end_time):
        # create base map
        return {"__start_time__": start_time, "__end_time__": end_time, "__error_msg__": "", "__count__": "" }

    # splunk returns lot of things. One of them is tag::eventtype which is excluded
    def __parse_results__(self, splunk_job, query, start_time, end_time, url_encoded_cols, include_internal_fields):
        # create base map
        base_mp = self.__create_empty_results_map__(query, start_time, end_time)

        # check for empty results
        results_total = int(splunk_job["resultCount"])
        job_id_trim = self.__get_splunk_job_display_id__(splunk_job)
        base_mp["__count__"] = str(results_total)
        base_mp["__error_msg__"] = ""
        utils.debug("SplunkSearch: __parse_results__: job_id: {}, query: {}, results_total: {}".format(job_id_trim, query, results_total))

        # check for empty results
        if (results_total == 0):
            # return base tsv
            return tsv.from_maps([base_mp])

        # define iterator variables
        results_offset = 0
        result_page_size = 100

        # special columns
        spl_internal_fields = ["_time"]

        # variable to store results
        results = []
        while results_offset < results_total:
            # append the result map to results
            reader = splunk_results.JSONResultsReader(splunk_job.results(count = result_page_size, offset = results_offset, output_mode="json"))
            # utils.info("reader.is_preview: {}".format(reader))
            for result in reader:
                # debugging
                if (len(results) % 1000 == 0):
                    utils.debug("SplunkSearch: __parse_results__: job_id: {}, result counter: {}".format(job_id_trim, len(results)))

                # check if it is a dict obect with results or some warning/error message
                if (isinstance(result, (dict))):
                    # keys starting with underscore _ seem to be internal to splunk and not useful here.
                    filtered_result = {}
                    for k in result.keys():
                        if ((k.startswith("_") == False and k.find("::") == -1) or k in spl_internal_fields):
                            if (isinstance(result[k], list)):
                                filtered_result[k] = ",".join(list(["{}".format(t) for t in result[k]]))
                            elif (isinstance(result[k], dict)):
                                utils.warn_once("SplunkSearch: key with dictionary as result type found. This is not fully supported: {}".format(k))
                                filtered_result[k] = json.dumps(result[k])
                            else:
                                filtered_result[k] = str(result[k])
                        else:
                            if (include_internal_fields == True):
                                filtered_result[k] = str(result[k])

                    # append filtered_result
                    results.append(filtered_result)
                    # utils.debug("__parse_results__: result: {}".format(result.keys()))
                elif (isinstance(result, (str))):
                    result_str = str(result)
                    if (result_str.find("WARN") == -1):
                        raise Exception("result of non dict type found: {}: {}".format(type(result), result_str))
                    else:
                        warn_msg = result_str if (len(result_str) <= 10) else result_str[0:10]
                        utils.info_once("Found a warning message: {}".format(warn_msg))
                elif (isinstance(result, (list))):
                    result_str = ",".join([str(t) for t in result])
                    utils.warn("Found a list type: {}".format(result))
                elif (isinstance(result, (splunk_results.Message))):
                    result_str = "{}".format(result)
                    warn_msg = result_str if (len(result_str) <= 10) else result_str[0:10]
                    utils.info_once("Found a warning message: {}".format(warn_msg))
                else:
                    raise Exception("result of non dict type found: {}".format(type(result)))

            # increment offset
            results_offset = results_offset + result_page_size

        # combine base results
        counter = 0
        for result in results:
            # debugging
            counter = counter + 1
            if (counter % 1000 == 0):
                utils.debug("__parse_results__: converting to sensor format counter: {}".format(counter))

            # convert
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
                if (url_encoded_cols is not None and key2 in url_encoded_cols):
                    key2 = "{}:url_encoded".format(key2)
                    value2 = utils.url_encode(value2)

                # assign to new map
                result2[key2] = utils.strip_spl_white_spaces(str(value2))

            # append to array
            results2.append(result2)

        # construct tsv from the list of hashmaps
        return tsv.from_maps(results2)
        
    def __resolve_time_str__(self, x):
        # check for specific syntax with now
        if (x.startswith("now")):
            x = x.replace(" ", "")
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
    def __init__(self, header, data, splunk_search = None, host = None, app = None, username = None, password = None, cookie = None, timeout_sec = 600, wait_sec = 10, attempts = 3,
        enable_cache = False, use_partial_results = False, num_par = 0, attempt_sleep_sec = 30):
        super().__init__(header, data)

        # check if new splunk search instance is needed
        if (splunk_search is None):
            splunk_search = SplunkSearch(host, app = app, username = username, password = password, cookie = cookie, timeout_sec = timeout_sec, wait_sec = wait_sec, attempts = attempts,
                enable_cache = enable_cache, use_partial_results = use_partial_results, attempt_sleep_sec = attempt_sleep_sec)

        self.splunk_search = splunk_search
        self.host = host
        self.app = app
        self.username = username 
        self.password = password
        self.cookie = cookie 
        self.timeout_sec = timeout_sec
        self.wait_sec = wait_sec
        self.attempts = attempts
        self.enable_cache = enable_cache
        self.use_partial_results = use_partial_results
        self.num_par = num_par
        self.attempt_sleep_sec = attempt_sleep_sec

    def get_events(self, query_filter, start_ts_col, end_ts_col, prefix, url_encoded_cols = None, include_internal_fields = False, limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "SplunkTSV: get_events")

        # create args and kwargs for multithreaded functional call
        args = (query_filter, start_ts_col, end_ts_col, prefix)
        kwargs = dict(url_encoded_cols = url_encoded_cols, include_internal_fields = include_internal_fields, limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg)

        # return
        return self \
            .extend_class(multithread_ext.MultiThreadTSV, num_par = self.num_par) \
                .parallelize(__get_events_par__, self.splunk_search, *args, **kwargs)

    def get_events_parsed(self, query, start_ts_col, end_ts_col, prefix, url_encoded_cols = None, include_internal_fields = False, limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "SplunkTSV: get_events_parsed")
        return self \
            .get_events(query, start_ts_col, end_ts_col, prefix, url_encoded_cols = url_encoded_cols, include_internal_fields = include_internal_fields,
                limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg) \
            .add_empty_cols_if_missing("{}:json_encoded".format(prefix), dmsg = dmsg) \
            .explode_json("{}:json_encoded".format(prefix), prefix = prefix, url_encoded_cols = url_encoded_cols, dmsg = dmsg)
             
def __get_events_par__(xtsv, xtsv_splunk_search, query_filter, start_ts_col, end_ts_col, prefix, url_encoded_cols = None, include_internal_fields = False,
    limit = None, num_par_on_limit = 0, dmsg = ""):

    dmsg = utils.extend_inherit_message(dmsg, "__get_events_par__")

    def __get_events_explode__(mp):
        # take start and end time
        start_time = mp[start_ts_col]
        end_time = mp[end_ts_col]

        # resolve query_filter
        query_filter_resolved = query_filter
        for c in xtsv.get_header_fields():
            cstr = "{" + c + "}"
            # replace if exists
            if (query_filter_resolved.find(cstr) != -1):
                query_filter_resolved = query_filter_resolved.replace(cstr, mp[c])

        # return
        mps = xtsv_splunk_search.call_search(query_filter_resolved, start_time, end_time = end_time, url_encoded_cols = url_encoded_cols,
            include_internal_fields = include_internal_fields, limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg).to_maps()
        json_mps = []
        for mp in mps:
            # create a new map
            json_mp = {}
            fields_mp = {}

            # add query execution parameters. the values are already sanitized
            for k in mp.keys():
                key2 = k
                value2 = str(mp[k])

                # assign to the correct map 
                if (__is_special_all_uppercase_field__(k) == False):
                    json_mp[k] = str(value2)
                else:
                    fields_mp[k] = str(value2)

            # add the blob of the event
            json_mp["json_encoded"] = utils.url_encode(json.dumps(fields_mp))

            # append
            json_mps.append(json_mp)
 
        # return json_mps
        return json_mps

    # TODO: This is not the correct place for this method
    def __is_special_all_uppercase_field__(x):
        return x[0].isupper() 

    # find which all columns are part of query_filter
    sel_cols = [start_ts_col, end_ts_col]
    for c in xtsv.get_header_fields():
            cstr = "{" + c + "}"
            # replace if exists
            if (query_filter.find(cstr) != -1 and c not in sel_cols):
                sel_cols.append(c)
   
    # return
    return xtsv \
        .explode(sel_cols, __get_events_explode__, prefix, collapse = False, default_val = "", dmsg = dmsg)

