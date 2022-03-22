# package to do web service REST calls in an efficient way
from omigo_core import tsv
from omigo_core import tsvutils
from omigo_core import utils 
from omigo_ext import multithread_ext

# TODO: selective_execution doesnt feel like a good design pattern.
class WebServiceTSV(tsv.TSV):
    def __init__(self, header, data, num_par = 0, timeout_sec = 5, num_batches = 10, status_check_interval_sec = 10, verify = True, enable_opt_exec = True, inherit_message = ""):
        super().__init__(header, data)
        self.num_par = num_par
        self.timeout_sec = timeout_sec
        self.num_batches = num_batches
        self.status_check_interval_sec = status_check_interval_sec
        self.verify = verify
        self.enable_opt_exec = enable_opt_exec
        self.inherit_message = inherit_message + ": WebServiceTSV" if (inherit_message != "") else "WebServiceTSV"

    def call_web_service(self, *args, **kwargs):
        return self \
            .extend_class(multithread_ext.MultiThreadTSV, num_par = self.num_par, num_batches = self.num_batches, status_check_interval_sec = self.status_check_interval_sec, inherit_message = self.inherit_message) \
            .parallelize(__call_web_service__, self.timeout_sec, self.verify, self.enable_opt_exec, self.inherit_message, *args, **kwargs)

# call web service function.
def __call_web_service__(xtsv, xtsv_timeout_sec, xtsv_verify, xtsv_enable_opt_exec, xtsv_inherit_message, url, prefix, query_params = None, header_params = None,
    body_params = None, username = None, password = None, include_resolved_values = False, selective_execution_func = None):

    # initialize variables
    if (query_params is None):
        query_params = {}

    if (header_params is None):
        header_params = {}

    # prepare a list of columns that need to be replaced
    url_cols = []
    query_params_cols = []
    header_params_cols = []
    body_params_cols = []
    all_sel_cols = []

    # iterate over all the columns and find where all they exist
    for c in xtsv.get_header_fields():
        cstr = "{" + c + "}"
        # check url
        if (url.find(cstr) != -1 and c not in url_cols):
            url_cols.append(c)
            all_sel_cols.append(c)

        # check query_params
        for k in query_params.keys():
            if (query_params[k].find(cstr) != -1 and c not in query_params_cols):
                query_params_cols.append(c)
                all_sel_cols.append(c)

        # check header_params
        for h in header_params.keys():
            if (header_params[h].find(cstr) != -1 and c not in header_params_cols):
                header_params_cols.append(c)
                all_sel_cols.append(c)

        # check body
        if (body_params is not None):
            if (isinstance(body_params, dict)):
                for k in body_params.keys():
                    if (body_params[k].find(cstr) != -1 and c not in body_params_cols):
                        body_params_cols.append(c)
                        all_sel_cols.append(c)
            elif (isinstance(body_params, str)):
                if (body_params.find(cstr) != -1 and c not in body_params_cols):
                    body_params_cols.append(c)
                    all_sel_cols.append(c)
            else:
                raise Exception("Unknown data type for body:", body_params)

    # distinct all_sel_cols
    all_sel_cols = list(set(all_sel_cols))

    # print
    utils.trace("{}: call_web_service: url: {}, query_params: {}".format(xtsv_inherit_message, str(url), str(query_params)))
    utils.trace("{}: call_web_service: url_cols: {}, query_params_cols: {}, header_params_cols: {}, body_params_cols: {}".format(xtsv_inherit_message, str(url_cols), str(query_params_cols),
        str(header_params_cols), str(body_params_cols)))

    # use the same inherit_message
    xtsv_inherit_message2 = xtsv_inherit_message + ": call_web_service" if (xtsv_inherit_message != "") else "call_web_service"

    # take only distinct all_sel_cols 
    hash_tsv = xtsv.select(all_sel_cols, inherit_message = xtsv_inherit_message2).distinct()

    # if the number of rows are different, print some stats
    if (hash_tsv.num_rows() < xtsv.num_rows()):
        utils.debug("{}: call_web_service: Number of rows: {}, Number of distinct rows for web service: {}, enable_opt_exec: {}".format(xtsv_inherit_message2,
            xtsv.num_rows(), hash_tsv.num_rows(), xtsv_enable_opt_exec))

    # avoid making duplicate calls to the web service by hashing the all_sel_cols
    if (xtsv_enable_opt_exec == True):
        # optimize the calls 
        hash_explode_tsv = hash_tsv \
            .explode(all_sel_cols, __call_web_service_exp_func__(xtsv_timeout_sec, xtsv_verify, url, query_params, header_params, body_params, username, password, url_cols,
                query_params_cols, header_params_cols, body_params_cols, include_resolved_values, selective_execution_func),
                prefix = prefix, collapse = False, inherit_message = xtsv_inherit_message2)
  
        # merge the results back to the original using map_join
        return xtsv.natural_join(hash_explode_tsv, inherit_message = xtsv_inherit_message2)
    else:
        # run transforms multiple times to generate resolved state of each variable
        return xtsv \
            .explode(all_sel_cols, __call_web_service_exp_func__(xtsv_timeout_sec, xtsv_verify, url, query_params, header_params, body_params, username, password, url_cols,
                query_params_cols, header_params_cols, body_params_cols, include_resolved_values, selective_execution_func),
                prefix = prefix, collapse = False, inherit_message = xtsv_inherit_message2)

# the explode func for web service
def __call_web_service_exp_func__(xtsv_timeout_sec, xtsv_verify, url, query_params, header_params, body_params, username, password, url_cols, query_params_cols, header_params_cols, body_params_cols,
    include_resolved_values, selective_execution_func):

    def __call_web_service_exp_func_inner__(mp):
        # resolve url
        url_resolved = url
        for c in url_cols:
            cstr = "{" + c + "}"
            url_resolved = url_resolved.replace(cstr, mp[c])

        # resolve query_params. Remember to not change the original hashmap
        query_params_resolved = {}
        for k in query_params.keys():
            query_params_resolved[k] = query_params[k]
            for c in query_params_cols:
                cstr = "{" + c + "}"
                query_params_resolved[k] = query_params_resolved[k].replace(cstr, mp[c])

        # resolve header_params
        header_params_resolved = {}
        for h in header_params.keys():
            header_params_resolved[h] = header_params[h]
            for c in header_params_cols:
                cstr = "{" + c + "}"
                header_params_resolved[h] = header_params_resolved[h].replace(cstr, mp[c])

        # resolve body_params
        if (body_params is not None):
            if (isinstance(body_params, dict)):
                body_params_resolved = {}
                for k in body_params.keys():
                    body_params_resolved[k] = body_params[k]
                    for c in body_params_cols:
                        cstr = "{" + c + "}"
                        body_params_resolved[k] = body_params_resolved[k].replace(cstr, mp[c])
            elif(isinstance(body_params, str)):
                body_params_resolved = body_params
                for c in body_params_cols:
                    cstr = "{" + c + "}"
                    body_params_resolved = body_params_resolved.replace(cstr, mp[c])
            else:
                raise Exception("Unknown data type for body_params:", body_params)
        else:
            body_params_resolved = None

        # shorter version of body_params_resolved
        body_params_resolved_strip = body_params_resolved[0:40] + "..." if (body_params_resolved is not None and len(body_params_resolved) >= 40) else body_params_resolved

        # debug
        utils.trace("__call_web_service_exp_func_inner__: mp: {}".format(mp))
        utils.trace("__call_web_service_exp_func_inner__: url: {}, query_params: {}, body_params: {}".format(url, query_params, body_params)) 
        utils.trace("__call_web_service_exp_func_inner__: url_resolved: {}, query_params_resolved: {}, body_params_resolved: {}".format(url_resolved,
            query_params_resolved, body_params_resolved_strip)) 

        # create response map
        result_mp = {}

        # check if there is special execution logic
        do_execute = True
        if (selective_execution_func is not None and selective_execution_func(mp) == False):
            do_execute = False

        # check whether to execute or not
        if (do_execute == True):
            # call web service. TODO: Need HTTP Response codes for better error handling, back pressure etc
            resp_str, resp_status_code, resp_err = tsvutils.read_url_response(url_resolved, query_params_resolved, header_params_resolved, body = body_params_resolved,
                username = username, password = password, timeout_sec = xtsv_timeout_sec, verify = xtsv_verify)
            result_mp["response:success"] = "1" if (str(resp_status_code).startswith("2")) else "0"
            result_mp["response:selective_execution"] = "1"
        else:
            # for some reason, this row is not meant to be executed. Ignore.
            resp_str, resp_status_code, resp_err = "", 0, ""
            result_mp["response:success"] = "0"
            result_mp["response:selective_execution"] = "0"
        
        # fill rest of the result map.
        result_mp["response:url_encoded"] = str(utils.url_encode(resp_str))
        result_mp["response:status_code"] = str(resp_status_code)
        result_mp["response:error"] = str(resp_err)

        # additioanl debugging information. 
        if (include_resolved_values == True):
            result_mp["request:url"] = url_resolved
            result_mp["request:query_params"] = str(query_params_resolved)

            # include the selection cols
            for k in mp.keys():
                result_mp[k] = str(mp[k]) 

        # create result
        combined_result = [result_mp]

        # return
        return combined_result

    # return the inner function
    return __call_web_service_exp_func_inner__

