from omigo_core import dataframe, utils, funclib
from jira import JIRA
import os
import json

# env variables
JIRA_API_USER = "JIRA_API_USER"
JIRA_API_PASS = "JIRA_API_PASS" # nosec
JIRA_API_AUTH_TOKEN = "JIRA_API_AUTH_TOKEN"

# Create a list of selected columns as JIRA has lot of noise
SELECTED_COLS = ["assignee", "attachment", "components", "created", "comment", "description", "issuetype", "labels", "project", "reporter", "resolution", "resolutiondate",
    "status", "summary", "updated"]
URL_ENCODED_COLS = ["assignee", "attachment", "comment", "components", "creator", "description", "reporter", "summary", "assignee:name", "assignee:displayName", "project:name",
    "reporter:name", "reporter:displayName"]

# Some jira fields are maps, take only relevant cols
SELECTED_COLS_MAP = {
    "assignee": ["name", "displayName"],
    "issuetype": ["name"],
    "project": ["key", "name"],
    "reporter": ["name", "displayName"],
    "resolution": ["name"],
    "status": ["name"]
}

# expected columns after url encoded mapping
EXPECTED_COLS = funclib.simple_map_to_url_encoded_col_names(["key", "assignee:name", "assignee:displayName", "attachment", "created", "comment", "issuetype:name", "labels", "project:key",
    "project:name", "reporter:name", "reporter:displayName", "resolution:name", "resolutiondate", "status:name", "summary", "description", "updated"], url_encoded_cols = URL_ENCODED_COLS)

SPECIAL_COLS = ["attachment", "comment", "components"]

# api handler for searching jira
class JiraSearch:
    def __init__(self, server = None, username = None, password = None, auth_token = None, verify = True):
        # warn
        utils.warn_once("JiraSearch: This is work in progress in extensions package. Some of the constants need to be decoupled")

        # validation
        if (server is None):
            raise Exception("JiraSearch: server is None")

        # init
        self.server = server
        self.jira_instance = None

        # instantiate and return
        jira_options = {"server": self.server, "verify": verify, "headers": {'content-type': 'application/json'}}
        utils.info("JiraSearch: instantiating with options: {}".format(jira_options))

        # check for credentials
        if (username is not None and password is not None):
            self.jira_instance = JIRA(jira_options, basic_auth = (username, password))
        elif (auth_token is not None):
            self.jira_instance = JIRA(jira_options, token_auth = auth_token)
        else:
            raise Exception("JiraSearch: No valid authentication mechanism found")

    def get_server(self):
        return self.server

    def search_issues(self, query, extra_cols = None, url_encoded_cols = URL_ENCODED_COLS, max_results = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "JiraSearch: search_issues")

        # resolve num_warnings
        max_results = int(utils.resolve_default_parameter("max_results", max_results, "10", "{}: {}".format(dmsg, query)))

        # the query is assumed to be resolved
        search_results = self.jira_instance.search_issues(query, maxResults = max_results)

        # expected cols in result
        expected_cols = EXPECTED_COLS
        if (extra_cols is not None and len(extra_cols) > 0):
            expected_cols = sorted(list(set(expected_cols + funclib.simple_map_to_url_encoded_col_names(extra_cols, url_encoded_cols = extra_cols))))

        # selected cols
        selected_cols = SELECTED_COLS
        if (extra_cols is not None and len(extra_cols) > 0):
            selected_cols = sorted(list(set(selected_cols + extra_cols)))

        # check for empty
        if (search_results is None or len(search_results) == 0):
            return dataframe.new_with_cols(expected_cols)

        # iterate
        result_xdfs = []
        for search_result in search_results:
            # jira has a special attribute that holds all fields
            key = search_result.raw["key"]
            result_fields = search_result.raw["fields"]

            # trace
            # for k in result_fields.keys():
            #     value = str(result_fields[k]) if (result_fields[k] is not None) else ""
            #     if (value != "" and value != "None" and value != "null"):
            #         utils.trace("JiraSearch: search_issues: key: {}, value: {}".format(k, value.replace("\n", "")[0:50] + "..."))

            # iterate and add each available field
            mp = {}
            mp_encoded = {}

            # assign key
            mp["key"] = str(key)

            # iterate on selected cols
            for k in selected_cols:
                # check for presence of key
                if (k in result_fields.keys() and result_fields[k] is not None):
                    value = result_fields[k]

                    # check for extra cols first, they are url encoded by default and no parsing here
                    if ((extra_cols is not None and k in extra_cols) or k in SPECIAL_COLS):
                        mp[k] = json.dumps({"value": value})
                    else:
                        # check for the type of value
                        if (isinstance(value, (dict))):
                            mp_value = value
                            sel_keys = SELECTED_COLS_MAP[k] if (k in SELECTED_COLS_MAP) else None

                            # use selected keys if defined, else the entire blob
                            if (sel_keys is not None):
                                for k2 in sel_keys:
                                    value2 = mp_value[k2] if (k2 in mp_value.keys()) else ""
                                    mp["{}:{}".format(k, k2)] = str(value2)
                            else:
                                utils.warn("JiraSearch: search_issues: map found without specific columns mapping: {}".format(value))
                                mp[k] = str(value)
                        elif (isinstance(value, (list))):
                            list_value = value
                            mp[k] = ",".join([str(t) for t in list_value])
                        elif (isinstance(value, (str, int, float))):
                            mp[k] = str(value)
                        else:
                            utils.warn("JiraSearch: search_issues: unknown value data type: {}, {}".format(value, type(value)))
                            mp[k] = str(value)

            # do the url encoding
            for k in mp.keys():
                if (url_encoded_cols is not None and k in url_encoded_cols):
                    mp_encoded["{}:url_encoded".format(k)] = utils.url_encode(mp[k])
                elif (extra_cols is not None and k in extra_cols):
                    mp_encoded["{}:url_encoded".format(k)] = utils.url_encode(mp[k])
                else:
                    mp_encoded[k] = mp[k]

            # append to tsvs
            result_xdfs.append(dataframe.from_maps([mp_encoded]))

        # merge
        result = dataframe.merge_union(result_xdfs) \
            .add_empty_cols_if_missing(expected_cols)

        # return
        return result

# DF for search jira
class JiraDF(dataframe.DataFrame):
    def __init__(self, header, data_fields, jira_search = None, server = None, username = None, password = None, auth_token = None, verify = True):
        super().__init__(header, data_fields)

        # instantiate
        self.jira_search = jira_search if (jira_search is not None) else JiraSearch(server = server, username = username, password = password, auth_token = auth_token, verify = verify)

    def search_issues(self, query_template, prefix, extra_cols = None, url_encoded_cols = URL_ENCODED_COLS, max_results = 10, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "JiraDF: search_issues")

        def __search_issues_explode_func__(mp):
            # resolve query
            query = utils.replace_template_props(mp, query_template)
            utils.info("JiraDF: search_issues: resolved query: {}".format(query))

            # call jira search
            results = self.jira_search.search_issues(query, extra_cols = extra_cols, max_results = max_results, dmsg = dmsg)

            # return
            return results.to_maps()

        # return
        return self \
            .explode(".*", lambda mp: __search_issues_explode_func__(mp), prefix, collapse = False, dmsg = dmsg)

