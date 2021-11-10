from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import base64
import json

from tsv_data_analytics import tsv
from tsv_data_analytics import utils

def __read_base_url(url, query_params = {}, headers = {}, username = None, password = None):
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

    return response

def read_url_json(url, query_params = {}, headers = {}, username = None, password = None):
    # read response
    response = __read_base_url(url, query_params, headers, username, password)

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


def read_url(url, query_params = {}, headers = {}, sep = None, username = None, password = None):
    # best effort detection for separator
    if (url.endswith(".csv") and sep == None):
        sep = ","

    # read response
    response = __read_base_url(url, query_params, headers, username, password)

    # check for content type
    if ("content-type" in response.headers and response.headers["content-type"].startswith("text/plain") or "Content-Type" in response.headers and response.headers["Content-Type"].startswith("text/plain")):
        response_str = response.read().decode("ascii")
        # split into lines
        lines = response_str.split("\n")
        header = lines[0]
        data = lines[1:]

        # check for other separators
        if (sep != None):
            if ("\t" in response_str):
                raise Exception("Cant parse non tab separated file as this contains tabs")
            # replace
            header = header.replace(sep, "\t")
            data = [x.replace(sep, "\t") for x in data]
        # return
        return tsv.TSV(header, data)
    else:
        raise Exception("Unable to parse the text response:", str(response.headers).split("\n"))



