# package to do web service REST calls in an efficient way
from tsv_data_analytics import tsv
from tsv_data_analytics import tsvutils
from tsv_data_analytics import utils 

class WebServiceTSV(tsv.TSV):
    def __init__(self, header, data, timeout_sec = 5):
        super().__init__(header, data)
        self.timeout_sec = timeout_sec

    def __call_web_service_exp_func__(self, url, query_params, header_params, username, password, url_cols, query_params_cols, header_params_cols, new_col, include_resolved_values):
        def __call_web_service_exp_func_inner__(mp):
            # resolve url
            url_resolved = url
            for c in url_cols:
                cstr = "{" + c + "}"
                url_resolved = url_resolved.replace(cstr, mp[c])

            # resolve header_params
            header_params_resolved = header_params
            for c in header_params_cols:
                cstr = "{" + c + "}"
                for h in header_params_resolved.keys():
                    header_params_resolved[h] = header_params_resolved[h].replace(cstr, mp[c])

            # resolve query_params
            query_params_resolved = query_params
            for c in query_params_cols:
                cstr = "{" + c + "}"
                for k in query_params_resolved.keys():
                    query_params_resolved[k] = query_params_resolved[k].replace(cstr, mp[c])

            # call web service. TODO: Need HTTP Response codes for better error handling, back pressure etc
            response_str = ""
            response_err = ""
            try:
                response_str = tsvutils.read_url_response(url_resolved, query_params_resolved, header_params_resolved, username, password, timeout_sec = self.timeout_sec)
            except BaseException as err:
                print(err)
                response_err = str(err)
                
            # create response map          
            result_mp = {}
            if (response_str == None):
                response = ""
            result_mp[new_col + ":url_encoded"] = utils.url_encode(response_str)
            result_mp[new_col + ":error"] = response_err

            # additioanl debugging information
            if (include_resolved_values == True):
                result_mp[new_col + ":url"] = url_resolved
                result_mp[new_col + ":query_params"] = str(query_params_resolved)
                result_mp[new_col + ":header_params"] = str(header_params_resolved)

            # return
            return [result_mp]

        # return the inner function
        return __call_web_service_exp_func_inner__

    def call_web_service(self, url, new_col, cols = None, query_params = None, header_params = None, username = None, password = None, include_resolved_values = False):
        # resolve cols
        if (cols == None):
            sel_cols = self.get_header_fields()
        else:
            sel_cols = self.__get_matching_cols__(cols)

        # validation
        if (new_col in self.get_header_fields()):
            raise Exception("New column already exists:", new_col, str(self.get_header_fields()))

        # initialize variables
        if (query_params == None):
            query_params = {}

        if (header_params == None):
            header_params = {}

        # prepare a list of columns that need to be replaced
        url_cols = []
        query_params_cols = []
        header_params_cols = []
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

        # distinct all_sel_cols
        all_sel_cols = list(set(all_sel_cols))

        # print
        utils.debug("call_web_service: url_cols: {}, query_params_cols: {}, header_params_cols: {}".format(str(url_cols), str(query_params_cols), str(header_params_cols)))

        # do some sanity check
        if (cols != None and len(sel_cols) != len(all_sel_cols)):
            diff_cols = set(sel_cols).difference(set(all_sel_cols))
            utils.warn("call_web_service: not all selected columns are used. Unused: {}".format(str(diff_cols)))

        # run transforms multiple times to generate resolved state of each variable
        return self \
            .explode(all_sel_cols, self.__call_web_service_exp_func__(url, query_params, header_params, username, password, url_cols, query_params_cols, header_params_cols, new_col, include_resolved_values)) \
            .remove_suffix("__call_web_service_exp_func_inner__")
