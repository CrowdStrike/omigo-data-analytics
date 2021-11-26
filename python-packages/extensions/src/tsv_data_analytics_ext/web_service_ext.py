# package to do web service REST calls in an efficient way
from tsv_data_analytics import tsv
from tsv_data_analytics import tsvutils
from tsv_data_analytics import utils 

class WebServiceTSV(tsv.TSV):
    def __init__(self, header, data, timeout_sec = 5):
        super().__init__(header, data)
        self.timeout_sec = timeout_sec

    def call_web_service(self, url, prefix, cols = None, query_params = None, header_params = None, body_params = None, username = None, password = None, include_resolved_values = False,
        selective_execution_func = None):

        # resolve cols
        if (cols == None):
            sel_cols = self.get_header_fields()
        else:
            sel_cols = self.__get_matching_cols__(cols)

        # initialize variables
        if (query_params == None):
            query_params = {}

        if (header_params == None):
            header_params = {}

        # prepare a list of columns that need to be replaced
        url_cols = []
        query_params_cols = []
        header_params_cols = []
        body_params_cols = []
        all_sel_cols = []

        # iterate over all the columns and find where all they exist
        for c in sel_cols:
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
            if (body_params != None):
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
        utils.debug("call_web_service: url: {}, query_params: {}, header_params: {}".format(str(url), str(query_params), str(header_params), str(body_params)))
        utils.debug("call_web_service: url_cols: {}, query_params_cols: {}, header_params_cols: {}, body_params_cols: {}".format(str(url_cols), str(query_params_cols), str(header_params_cols), str(body_params_cols)))

        # do some sanity check
        if (cols != None and len(sel_cols) != len(all_sel_cols)):
            diff_cols = set(sel_cols).difference(set(all_sel_cols))
            utils.warn("call_web_service: not all selected columns are used. Unused: {}".format(str(diff_cols)))

        # run transforms multiple times to generate resolved state of each variable
        return self \
            .explode(all_sel_cols, self.__call_web_service_exp_func__(url, query_params, header_params, body_params, username, password, url_cols,
                query_params_cols, header_params_cols, body_params_cols, include_resolved_values, selective_execution_func), prefix = prefix, collapse = False)

    def __call_web_service_exp_func__(self, url, query_params, header_params, body_params, username, password, url_cols, query_params_cols, header_params_cols, body_params_cols,
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
            if (body_params != None):
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

            # debug
            #utils.debug("__call_web_service_exp_func_inner__: mp: {}".format(mp))
            #utils.debug("__call_web_service_exp_func_inner__: url: {}, query_params: {}, header_params: {}, body_params: {}".format(url, query_params, header_params, body_params)) 
            #utils.debug("__call_web_service_exp_func_inner__: url_resolved: {}, query_params_resolved: {}, header_params_resolved: {}, body_params_resolved: {}".format(url_resolved, query_params_resolved, header_params_resolved, body_params_resolved)) 

            # create response map
            result_mp = {}

            # check if there is special execution logic
            do_execute = True
            if (selective_execution_func != None and selective_execution_func(mp) == False):
                do_execute = False

            # check whether to execute or not
            if (do_execute == True):
                # call web service. TODO: Need HTTP Response codes for better error handling, back pressure etc
                resp_str, resp_status_code, resp_err = tsvutils.read_url_response(url_resolved, query_params_resolved, header_params_resolved, body = body_params_resolved, username = username, password = password, timeout_sec = self.timeout_sec)
                result_mp["response:success"] = "1" if (str(resp_status_code).startswith("2")) else "0"
                result_mp["response:selective_execution"] = "1"
            else:
                # for some reason, this row is not meant to be executed. Ignore.
                resp_str, resp_status_code, resp_err = "", 0, ""
                result_mp["response:success"] = "0"
                result_mp["response:selective_execution"] = "0"
            
            # fill rest of the result map    
            result_mp["response:url_encoded"] = str(utils.url_encode(resp_str))
            result_mp["response:status_code"] = str(resp_status_code)
            result_mp["response:error"] = str(resp_err)

            # additioanl debugging information
            if (include_resolved_values == True):
                result_mp["request:url"] = url_resolved
                result_mp["request:query_params"] = str(query_params_resolved)
                result_mp["request:header_params"] = str(header_params_resolved)
                result_mp["request:body_params:url_encoded"] = utils.url_encode(body_params_resolved)
                for k in mp.keys():
                    result_mp[k] = str(mp[k]) 

            # return
            return [result_mp]

        # return the inner function
        return __call_web_service_exp_func_inner__

