"""general purpose extensions to work with tsv data over http"""

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import base64
import json

from tsv_data_analytics import tsv
from tsv_data_analytics import utils

def read_url(url, query_params = {}, headers = {}, username = None, password = None, accepted_cols = None, excluded_cols = None):
    # check for query params
    if (len(query_params) > 0):
        params_encoded_str = urlencode(query_params)
        url = "{}?{}".format(url, params_encoded_str)
    
    # create request        
    request = Request(url, headers = headers)

    # check for username, password authorization
    if (username != None and password != None and "Authorization" not in headers.keys()):
        # create the authorization header
        base64str = base64.b64encode("{}:{}".format(username, password).encode("ascii")).decode("ascii")
        request.add_header("Authorization", "Basic {}".format(base64str))

    # call urlopen and get response
    response = urlopen(request)
        
    # check for error code
    if (response.status != 200):
        raise exception("HTTP response is not 200 OK. Returning:" + response.msg)

    # check for content type
    if ("content-type" in response.headers and response.headers["content-type"] == "application/json"):
        response_str = response.read().decode("ascii")
        json_obj = json.loads(response_str)
        if (isinstance(json_obj, list)):
            lines = []
            for v in json_obj:
                lines.append(utils.url_encode(json.dumps(v)).replace("\n", " "))
            return tsv.TSV("json_encoded", "\n".join(lines))
                #.explode_json("json_encoded", suffix = "json", accepted_cols = accepted_cols, excluded_cols = excluded_cols)
                #.remove_suffix("json")
        elif (isinstance(json_obj, dict)):
            return tsv.TSV("json_encoded", [utils.url_encode(response_str).replace("\n", " ")])
                # .explode_json("json_encoded", suffix = "json", accepted_cols = accepted_cols, excluded_cols = excluded_cols)
                #.remove_suffix("json")
        else:
            raise Exception("Unable to parse the json response:", json_obj)
    else:
        raise Exception("Unable to parse the json response:", json_obj)


