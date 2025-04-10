from omigo_core import dataframe, utils
from jira import JIRA
import os
import json

# env variables
JIRA_API_USER = "JIRA_API_USER"
JIRA_API_PASS = "JIRA_API_PASS" # nosec
JIRA_API_AUTH_TOKEN = "JIRA_API_AUTH_TOKEN"

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
        self.fields_mapping = {}

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

        # build fields mapping
        for field in self.jira_instance.fields():
            field_name = str(field["name"])
            field_type = str(field["schema"]["type"]) if ("schema" in field and "type" in field["schema"]) else ""
            self.fields_mapping[str(field["id"])] = {"name": field_name, "type": field_type, "field": field}

    def get_server(self):
        return self.server

    def get_instance(self):
        return self.jira_instance

    def search_issues(self, query, max_results = None, datatype_map = None, dmsg = ""):
        dmsg = utils.extend_inherit_message(dmsg, "JiraSearch: search_issues")

        # resolve num_warnings
        max_results = int(utils.resolve_default_parameter("max_results", max_results, "10", "{}: {}".format(dmsg, query)))
        datatype_map = utils.resolve_default_parameter("datatype_map", datatype_map, {}, "{}: {}".format(dmsg, query))

        # the query is assumed to be resolved
        search_results = self.jira_instance.search_issues(query, maxResults = max_results)

        # check for empty
        if (search_results is None or len(search_results) == 0):
            return dataframe.create_empty()

        # iterate
        result_xdfs = []
        for search_result in search_results:
            # jira has a special attribute that holds all fields
            key = search_result.raw["key"]
            result_fields = search_result.raw["fields"]

            # trace
            for k in result_fields.keys():
                value = str(result_fields[k]) if (result_fields[k] is not None) else ""

            # iterate and add each available field
            mp = {}

            # assign key
            mp["key"] = str(key)

            # iterate on selected cols
            for k in result_fields.keys():
                # take value
                value = result_fields[k]

                # Ignore None as even dictionary can be None
                if (value is None):
                    value = ""
                    continue

                if (isinstance(value, (list)) and len(value) == 0):
                    value = ""
                    continue

                if (isinstance(value, (dict)) and len(value) == 0):
                    value = ""
                    continue

                # map the key
                k2 = k
                field_type = self.fields_mapping[k]["type"] if (k in self.fields_mapping) else ""

                # map the custom fields
                if (k.startswith("customfield_") and k in self.fields_mapping):
                    k2 = self.fields_mapping[k]["name"]


                # do json encoding if needed
                if (field_type in ("string")):
                    value_str = str(value)
                    if (value_str.startswith("{\"") and value_str.endswith("}")):
                        mp_value = json.dumps(json.loads(value_str))
                        mp["{}:json_encoded".format(k2)] = mp_value 
                    elif (value_str.startswith("[{\"") and value_str.endswith("}]")):
                        mp_value = json.dumps(json.loads(value_str))
                        mp["{}:json_encoded".format(k2)] = mp_value 
                    else:
                        mp[k2] = value_str
                elif (field_type in ("date", "datetime", "group", "number", "resolution")):
                    mp[k2] = str(value)
                elif (isinstance(value, (dict))):
                    mp_value = json.dumps(value)
                    mp["{}:json_encoded".format(k2)] = mp_value 
                elif (field_type in ("array") and len(value) > 0):
                    if (isinstance(value[0], (dict))):
                        mp_value = json.dumps(value)
                        mp["{}:json_encoded".format(k2)] = mp_value
                    else: 
                        mp[k2] = ",".join(list([str(v) for v in value]))
                elif (isinstance(value, (str, int, float))):
                    mp[k2] = str(value)
                else:
                    mp_vars = vars(value)
                    mp_value = {}
                    for k in mp_vars:
                        if (k.startswith("_") == False):
                            mp_value[str(k)] = str(mp_vars[k])
                    mp["{}:json_encoded".format(k2)] = json.dumps(mp_value)

            # inner transformation function
            def __raw_json_custom_field_mapping__(raw_json):
                # base condition
                if (raw_json is None or isinstance(raw_json, (dict)) == False):
                    return raw_json

                # transform
                mp_new = {}
                for k in raw_json:
                    if (k.startswith("customfield_") and k in self.fields_mapping):
                        mp_new[self.fields_mapping[k]["name"]] = __raw_json_custom_field_mapping__(raw_json[k])
                    else:
                        mp_new[k] = __raw_json_custom_field_mapping__(raw_json[k])

                # return
                return mp_new

            # append raw along with custom field transformation
            mp["raw"] = json.dumps(__raw_json_custom_field_mapping__(search_result.raw))

            # append to tsvs
            result_xdfs.append(dataframe.from_maps([mp]))

        # merge
        result = dataframe.merge_union(result_xdfs)

        # return
        return result

# DF for search jira
class JiraDF(dataframe.DataFrame):
    def __init__(self, header, data_fields, jira_search = None, server = None, username = None, password = None, auth_token = None, verify = True):
        super().__init__(header, data_fields)

        # instantiate
        self.jira_search = jira_search if (jira_search is not None) else JiraSearch(server = server, username = username, password = password, auth_token = auth_token, verify = verify)

    def search_issues(self, query_template, prefix, extra_cols = None, max_results = 10, dmsg = ""):
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

