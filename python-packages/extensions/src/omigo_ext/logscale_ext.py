from omigo_core import tsv, utils, timefuncs, dataframe
from omigo_ext import multithread_ext
import datetime
from dateutil import parser
import os
import json
import time
import math
import traceback
# !{sys.executable} -m pip install --upgrade humiolib
from humiolib.HumioClient import HumioClient

# Basic search class
class LogScaleSearch:
    def __init__(self, base_url = None, repository = None, user_token = None, timeout_sec = 600, wait_sec = 10, attempts = 3, attempt_sleep_sec = 30):
        # Validation
        if (base_url is None):
            raise Exception("Missing parameters: base_url")

        # check for repository
        if (repository is None):
            raise Exception("Missing parameters: repository")

        # check for user_token
        if (user_token is None):
            raise Exception("Missing parameters: user_token")

        # initialize parameters
        self.base_url = base_url
        self.repository = repository
        self.user_token= user_token
        self.timeout_sec = timeout_sec
        self.wait_sec = wait_sec
        self.attempts = attempts
        self.attempt_sleep_sec = attempt_sleep_sec

        # message at creating a new instance
        utils.debug("LogScaleSearch: new instance created: base_url: {}, repository: {}, timeout_sec: {}, wait_sec: {}, attempts: {}, attempt_sleep_sec: {}".format(
            self.base_url, self.repository, self.timeout_sec, self.wait_sec, self.attempts, self.attempt_sleep_sec))

        # warn
        utils.warn_once("LogScaleSearch: This is an initial version inspired from splunk_ext. Need to be optimized further.")

    def get_logscale_client(self):
        # create client once
        return HumioClient(base_url = self.base_url, repository = self.repository, user_token = self.user_token)

    def call_search(self, query, start_time, end_time = None, accepted_cols = None, excluded_cols = None, url_encoded_cols = None, limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "LogScaleSearch: call_search")

        # set default
        if (end_time is None):
            end_time = "now"

        # resolve time (probably again)
        start_time = self.__resolve_time_str__(start_time)
        end_time = self.__resolve_time_str__(end_time)

        # convert this to millis
        start_time_millis = timefuncs.datetime_to_utctimestamp_sec(start_time) * 1000
        end_time_millis = timefuncs.datetime_to_utctimestamp_sec(end_time) * 1000

        # debug
        utils.info("call_search: query: {}, start_time: {}, end_time: {}".format(query, start_time, end_time))

        # execute
        return self.__execute_query__(query, start_time_millis, end_time_millis, self.attempts, accepted_cols = accepted_cols, excluded_cols = excluded_cols,
            url_encoded_cols = url_encoded_cols, limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg)

    # get splunk job id for display
    def __get_logscale_job_display_id__(self, logscale_job):
        if (logscale_job is not None):
            return logscale_job.query_id
        else:
            return ""

    # inner method for calling query
    def __execute_query__(self, query, start_time_millis, end_time_millis, attempts_remaining, accepted_cols = None, excluded_cols = None,
        url_encoded_cols = None, limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "LogScaleSearch: __execute_query__")

        # call and return
        return self.__execute_normal_query__(query, start_time_millis, end_time_millis, accepted_cols, excluded_cols, url_encoded_cols, attempts_remaining,
            limit, num_par_on_limit, dmsg = dmsg)

    def __split_time_slots_millis__(self, st_millis, et_millis, num_splits):
        # find time width
        width = int(math.floor((et_millis - st_millis) / num_splits))

        # create slots
        slots = []
        for i in range(num_splits):
            st2 = st_millis + i * width
            et2 = st_millis + (i + 1) * width
            slots.append((st2, et2))

        # return
        return slots

    def __execute_normal_query__(self, query, start_time_millis, end_time_millis, accepted_cols, excluded_cols, url_encoded_cols, attempts_remaining, limit, num_par_on_limit, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "LogScaleSearch: __execute_normal_query__")

        # execute query
        # try:
        # submit job
        logscale_job = self.get_logscale_client().create_queryjob(query, start = start_time_millis, end = end_time_millis, is_live = False)
        job_id_trim = self.__get_logscale_job_display_id__(logscale_job)
        utils.info("{}: LogScale Job submitted: {}, start_time: {}, end_time: {}".format(utils.max_dmsg_str(dmsg), job_id_trim,
            timefuncs.utctimestamp_to_datetime_str(start_time_millis), timefuncs.utctimestamp_to_datetime_str(end_time_millis)))

        # create result
        events = []
        results_total = 0

        # note start time
        exec_start_time = timefuncs.get_utctimestamp_sec()

        # check for job to be ready. not sure what this does, just following example
        for poll_result in logscale_job.poll_until_done():
            # debug
            extraData = poll_result.metadata["extraData"] if ("extraData" in poll_result.metadata) else None
            hasMoreEvents = extraData["hasMoreEvents"] if (extraData is not None and "hasMoreEvents" in extraData) else None

            # debug
            utils.debug("{}: job: {}, eventCount: {}, hasMoreEvents: {}".format(utils.max_dmsg_str(dmsg), job_id_trim, poll_result.metadata["eventCount"], hasMoreEvents))

            # count total results
            results_total = results_total + poll_result.metadata["eventCount"]

            # append to result
            for event in poll_result.events:
                events.append(event)

        # end time
        exec_end_time = timefuncs.get_utctimestamp_sec()

        # debug
        utils.info("{}: job_id: {}, event count: {}, query time taken: {} secs".format(dmsg, job_id_trim, len(events), (exec_end_time - exec_start_time)))

        # check if limit is defined
        result = None
        if (limit is not None and num_par_on_limit > 1):
            # compare with limit
            if (results_total >= limit):
                if (num_par_on_limit > 1):
                    utils.warn("{}: limit: {} reached, splitting the query into {} parts and running again".format(dmsg, results_total, num_par_on_limit))
                    # dont make furthen than 1 level deep call
                    limit2 = None
                    num_par_on_limit2 = 0

                    # split the time range into num_par_on_limit slots
                    time_slots = self.__split_time_slots_millis__(start_time_millis, end_time_millis, num_par_on_limit)
                    xtsv_results = []

                    # iterate
                    for (st, et) in time_slots:
                        xtsv_result = self.__execute_normal_query__(query, st, et, accepted_cols, excluded_cols, url_encoded_cols, attempts_remaining,
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
            result = self.__parse_results__(logscale_job, events, query, start_time_millis, end_time_millis, accepted_cols, excluded_cols, url_encoded_cols)

        # cancel the job
        # logscale_job.cancel()

        # return
        return result
        # except Exception as e:
        #     # check if multiple attempts are needed
        #     if (attempts_remaining > 0):
        #         # debug
        #         utils.warn("{}: caught exception: {}, attempts remaining: {}".format(utils.max_dmsg_str(dmsg), str(e), attempts_remaining))
        #         # utils.error("{}: Stack Trace: {}".format(utils.max_dmsg_str(dmsg), traceback.format_exc()))

        #         # for gateway timeout do a longer wait by default
        #         if (str(e).find("HTTP 504 Gateway Time-out") != -1):
        #             # call again with a different timeout
        #             attempt_multiplier = int(math.min(self.attempts - attempts_remaining, 10))
        #             utils.info("{}: Gateway timeout: Sleeping for {} seconds before attempting again".format(dmsg, self.attempt_gateway_timeout_sleep_sec * attempt_multiplier))
        #             time.sleep(self.attempt_gateway_timeout_sleep_sec * attempt_multiplier)
        #         else:
        #             # call again with lesser attempts_remaining
        #             utils.info("{}: Sleeping for {} seconds before attempting again".format(dmsg, self.attempt_sleep_sec))
        #             time.sleep(self.attempt_sleep_sec)

        #         # return
        #         return self.__execute_normal_query__(query, start_time_millis, end_time_millis, accepted_cols, excluded_cols, url_encoded_cols, attempts_remaining - 1, limit,
        #             num_par_on_limit, dmsg = dmsg)
        #     else:
        #         utils.error("{}: Exception: {}".format(utils.max_dmsg_str(dmsg), str(e)))
        #         base_mp = self.__create_empty_results_map__(query, start_time_millis, end_time_millis)
        #         base_mp["__count__"] = "0"
        #         base_mp["__error_msg__"] = str(e)
        #         result = dataframe.from_maps([base_mp])

        #         # cancel the job
        #         # logscale_job.cancel()

        #         # return
        #         return result

    def __create_empty_results_map__(self, query, start_time_millis, end_time_millis):
        # create base map
        return {"__start_time__": start_time_millis, "__end_time__": end_time_millis, "__error_msg__": "", "__count__": "" }

    # splunk returns lot of things. One of them is tag::eventtype which is excluded
    def __parse_results__(self, logscale_job, events, query, start_time_millis, end_time_millis, accepted_cols, excluded_cols, url_encoded_cols):
        # create base map
        base_mp = self.__create_empty_results_map__(query, start_time_millis, end_time_millis)

        # check for empty results
        results_total = len(events)
        job_id_trim = self.__get_logscale_job_display_id__(logscale_job)
        base_mp["__count__"] = str(results_total)
        base_mp["__error_msg__"] = ""
        utils.debug("LogScaleSearch: __parse_results__: job_id: {}, query: {}, results_total: {}".format(job_id_trim, query, results_total))

        # check for empty results
        if (results_total == 0):
            # return base tsv
            return dataframe.from_maps([base_mp])

        # define iterator variables
        results_offset = 0
        result_page_size = 100

        # variable to store results
        results = []

        # combine base results
        counter = 0
        for event in events:
            # debugging
            counter = counter + 1
            if (counter % 1000 == 0):
                utils.debug("__parse_results__: converting to sensor format counter: {}".format(counter))

            # initialize
            result = {}

            # convert
            for k in base_mp.keys():
                result[k] = str(base_mp[k])

            # assign key-values with modified name
            for k in event.keys():
                # take original value
                key2 = k
                value2 = event[k]

                # some of the keys have @ and # as prefix. Remove those prefixes
                key2 = key2.replace("@", "").replace("#", "")

                # assign to new map
                result[key2] = utils.replace_spl_white_spaces_with_space(str(value2))

            # append to array
            results.append(result)

        # construct tsv from the list of hashmaps
        return dataframe.from_maps(results, accepted_cols = accepted_cols, excluded_cols = excluded_cols, url_encoded_cols = url_encoded_cols)

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
            return timefuncs.utctimestamp_to_datetime_str(int(base_time.timestamp()) - diff_sec)
        else:
            return timefuncs.utctimestamp_to_datetime_str(timefuncs.datetime_to_utctimestamp_sec(x))

# class to do data manipulation on TSV
class LogScaleTSV(tsv.TSV):
    def __init__(self, header, data, logscale_client = None, base_url = None, repository = None, user_token = None, timeout_sec = 600, wait_sec = 10, attempts = 3,
        num_par = 0, attempt_sleep_sec = 30):
        super().__init__(header, data)

        # check if new splunk search instance is needed
        if (logscale_client is None):
            logscale_client = LogScaleSearch(base_url, repository = repository, user_token = user_token, timeout_sec = timeout_sec, wait_sec = wait_sec, attempts = attempts)

        self.logscale_client = logscale_client
        self.base_url = base_url
        self.repository = repository
        self.user_token = user_token
        self.timeout_sec = timeout_sec
        self.wait_sec = wait_sec
        self.attempts = attempts
        self.num_par = num_par
        self.attempt_sleep_sec = attempt_sleep_sec

    def get_events(self, query_filter, start_ts_col, end_ts_col, prefix, url_encoded_cols = None, limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "LogScaleTSV: get_events")

        # create args and kwargs for multithreaded functional call
        args = (query_filter, start_ts_col, end_ts_col, prefix)
        kwargs = dict(url_encoded_cols = url_encoded_cols, limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg)

        # return
        return self \
            .extend_class(multithread_ext.MultiThreadTSV, num_par = self.num_par) \
                .parallelize(__get_events_par__, self.logscale_client, *args, **kwargs)

    def get_events_parsed(self, query, start_ts_col, end_ts_col, prefix, url_encoded_cols = None, limit = None, num_par_on_limit = 0, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "LogScaleTSV: get_events_parsed")
        return self \
            .get_events(query, start_ts_col, end_ts_col, prefix, url_encoded_cols = url_encoded_cols, limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg) \
            .add_empty_cols_if_missing("{}:json_encoded".format(prefix), dmsg = dmsg) \
            .explode_json("{}:json_encoded".format(prefix), prefix = prefix, url_encoded_cols = url_encoded_cols, dmsg = dmsg)

def __get_events_par__(xtsv, xtsv_logscale_search, query_filter, start_ts_col, end_ts_col, prefix, url_encoded_cols = None, limit = None, num_par_on_limit = 0, dmsg = ""):
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
        mps = xtsv_logscale_search.call_search(query_filter_resolved, start_time, end_time = end_time, url_encoded_cols = url_encoded_cols,
            limit = limit, num_par_on_limit = num_par_on_limit, dmsg = dmsg).to_maps()
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

