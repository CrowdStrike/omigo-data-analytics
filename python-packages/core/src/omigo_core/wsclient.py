import urllib
import json
import requests
from omigo_core import tsv, utils

# https://stackoverflow.com/questions/29931671/making-an-api-call-in-python-with-an-api-that-requires-a-bearer-token
class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self.token
        return r

# TODO: the body has to be a json payload. This is because of some bug in python requests.post api
# TODO: allow_redirects=False
# TODO: https://docs.python-requests.org/en/latest/user/quickstart/: Check the WARNINGS
def __read_base_url__(url, query_params = {}, headers = {}, body = None, username = None, password = None, api_token = None, timeout_sec = 120, verify = True, method = None, dmsg = ""):
    dmsg = utils.extend_inherit_message(dmsg, "__read_base_url__")

    # check for query params
    if (len(query_params) > 0):
        params_encoded_str = urllib.parse.urlencode(query_params)
        url = "{}?{}".format(url, params_encoded_str)

    # determine method
    if (method is None):
        if (body is None):
            method = "GET"
        else:
            method = "POST"

    # exception handling
    try:
        # call the web service
        if (method == "GET"):
            # make web service call
            if (username is not None and password is not None):
                response = requests.get(url, auth = (username, password), headers = headers, timeout = timeout_sec, verify = verify)
            elif (api_token is not None):
                response = requests.get(url, auth = BearerAuth(api_token), headers = headers, timeout = timeout_sec, verify = verify)
            else:
                response = requests.get(url, headers = headers, timeout = timeout_sec, verify = verify)
        elif (method == "POST"):
            # do some validation on body
            body_json = None
            try:
                body_json = json.loads(body)
            except Exception as e:
                utils.error("{}: body is not well formed json: {}".format(dmsg, body))
                raise e

            # make web service call
            if (username is not None and password is not None):
                response = requests.post(url, auth = (username, password), json = body_json, headers = headers, timeout = timeout_sec, verify = verify)
            elif (api_token is not None):
                response = requests.post(url, auth = BearerAuth(api_token), json = body_json, headers = headers, timeout = timeout_sec, verify = verify)
            else:
                response = requests.post(url, json = body_json, headers = headers, timeout = timeout_sec, verify = verify)
        elif (method == "PUT"):
            # do some validation on body
            body_json = None
            try:
                body_json = json.loads(body)
            except Exception as e:
                utils.error("{}: body is not well formed json: {}".format(dmsg, body))
                raise e

            # make web service call
            if (username is not None and password is not None):
                response = requests.put(url, auth = (username, password), json = body_json, headers = headers, timeout = timeout_sec, verify = verify)
            elif (api_token is not None):
                response = requests.put(url, auth = BearerAuth(api_token), json = body_json, headers = headers, timeout = timeout_sec, verify = verify)
            else:
                response = requests.put(url, json = body_json, headers = headers, timeout = timeout_sec, verify = verify)
        elif (method == "DELETE"):
            if (username is not None and password is not None):
                response = requests.delete(url, auth = (username, password), headers = headers, timeout = timeout_sec, verify = verify)
            elif (api_token is not None):
                response = requests.delete(url, auth = BearerAuth(api_token), headers = headers, timeout = timeout_sec, verify = verify)
            else:
                response = requests.delete(url, headers = headers, timeout = timeout_sec, verify = verify)
        else:
            raise Exception("Invalid method: {}".format(method))

        # return response
        return response, None
    except requests.exceptions.RequestException as e:
        utils.warn("{}: Found exception while making request: {}".format(dmsg, e))
        return None, e

# TODO: the semantics of this api are not clear
def read_url_json(url, query_params = {}, headers = {}, body = None, username = None, password = None, api_token = None, timeout_sec = 120, verify = True, method = None):
    utils.warn("read_url_json will flatten json that comes out as list. This api is still under development")

    # read response
    response_str, status_code, error_msg = read_url_response(url, query_params, headers, body = body, username = username, password = password, api_token = api_token,
        timeout_sec = timeout_sec, verify = verify, method = method)

    # construct header
    header = "\t".join(["json_encoded", "status_code", "error_msg"])
    data = []

    # look for error conditions
    if (status_code == 200):
        # parse json object
        json_obj = json.loads(response_str)

        # based on the type of the response, flatten the structure
        if (isinstance(json_obj, list)):
            # iterate and add as row
            for v in json_obj:
                fields = [utils.url_encode(json.dumps(v).replace("\n", " ")), str(status_code), str(error_msg)]
                data.append("\t".join(fields))
        elif (isinstance(json_obj, dict)):
            fields = [utils.url_encode(json.dumps(json_obj).replace("\n", " ")), str(status_code), str(error_msg)]
            data.append("\t".join(fields))
        else:
            fields = ["", "0", "Unable to parse the json response: {}".format(response_str)]
            data.append("\t".join(fields))
    else:
        fields = ["", "0", "Unable to parse the json response: {}".format(utils.url_encode(response_str).replace("\n", " "))]
        data.append("\t".join(fields))

    # return
    return tsv.TSV(header, data).validate()

def read_url_response(url, query_params = {}, headers = {}, body = None, username = None, password = None, api_token = None, timeout_sec = 120, verify = True, method = None,
    num_retries = 1, retry_sleep_sec = 1):
    # read response
    response, resp_exception = __read_base_url__(url, query_params, headers, body = body, username = username, password = password, api_token = api_token,
        timeout_sec = timeout_sec, verify = verify, method = method)

    # check for errors
    if (response is None):
        return "", 500, str(resp_exception)

    # check for error codes
    if (response.status_code != 200):
        # check for website saying too many requests (429) or service unavailable (503)
        if (response.status_code == 429 or response.status_code == 503):
            # too many requests. wait and try again.
            if (num_retries > 0):
                utils.debug("read_url_response: url: {}, query_params: {}, got status: {}. Attempts remaining: {}. Retrying after sleeping for {} seconds".format(url, query_params,
                    response.status_code, num_retries, retry_sleep_sec))
                time.sleep(retry_sleep_sec)
                return read_url_response(url, query_params = query_params, headers = headers, body = body, username = username, password = password, api_token = api_token,
                    timeout_sec = timeout_sec, verify = verify, method = method, num_retries = num_retries - 1, retry_sleep_sec = retry_sleep_sec * 2)
            else:
                utils.debug("read_url_response: url: {}, getting 429 too many requests or 5XX error. Use num_retries parameter to for backoff and retry.".format(url))
                return "", response.status_code, response.reason
        else:
            return "", response.status_code, response.reason

    # check for content type
    content_type = None
    if ("content-type" in response.headers):
        content_type = response.headers["content-type"].lower()
    elif ("Content-Type" in response.headers):
        content_type = response.headers["Content-Type"].lower()
    else:
        return "", 0, "Missing content-type: {}".format(str(response.headers))

    # best guess file type
    file_type = None
    if (url.endswith(".csv") or url.endswith(".tsv")):
        file_type = "text"
    elif (url.endswith(".gz")):
        file_type = "gzip"
    elif (url.endswith(".zip")):
        file_type = "zip"

    # get response
    response_str = None
    if (content_type.startswith("application/x-gzip-compressed") or content_type.startswith("application/gzip") or file_type == "gzip"):
        response_str = str(gzip.decompress(response.content), "utf-8").rstrip("\n")
    elif (content_type.startswith("application/x-zip-compressed") or content_type.startswith("application/zip") or file_type == "zip"):
        barr = response.content
        zfile = zipfile.ZipFile(BytesIO(barr))
        response_str = zfile.open(zfile.infolist()[0]).read().decode().rstrip("\n")
        zfile.close()
    elif (content_type.startswith("text/plain") or content_type.startswith("application/json") or file_type == "text"):
        response_str = response.content.decode().rstrip("\n")
    elif (content_type.startswith("application/octet-stream")):
        utils.warn("Content Type is octet stream. Using gzip.".format(content_type))
        response_str = str(gzip.decompress(response.content), "utf-8").rstrip("\n")
    else:
        utils.warn("Content Type is not known: {}. Using default decoding from str.decode().".format(content_type))
        response_str = response.content.decode().rstrip("\n")

    # return
    return response_str, response.status_code, ""

# TODO: Deprecated
def read_url(url, query_params = {}, headers = {}, sep = None, username = None, password = None, api_token = None, timeout_sec = 120, verify = True, method = None):
    utils.warn("This method name is deprecated. Use read_url_as_tsv instead")
    return read_url_as_tsv(url, query_params = query_params, headers = headers, sep = sep, username = username, password = password, api_token = api_token,
    timeout_sec = timeout_sec, verify = verify, method = method)

# TODO: the compressed file handling should be done separately in a function
def read_url_as_tsv(url, query_params = {}, headers = {}, sep = None, username = None, password = None, api_token = None, timeout_sec = 120, verify = True, method = None):
    # use the file extension as alternate way of detecting type of file
    # TODO: move the file type and extension detection to separate function
    file_type = None
    if (url.endswith(".csv") or url.endswith(".tsv")):
        file_type = "text"
    elif (url.endswith(".gz")):
        file_type = "gzip"
    elif (url.endswith(".zip")):
        file_type = "zip"

    # detect extension
    ext_type = None
    if (url.endswith(".csv") or url.endswith(".csv.gz") or url.endswith(".csv.zip")):
        ext_type = "csv"
        utils.warn("CSV file detected. Only simple csv files are supported where comma and tabs are not part of any data.")
    elif (url.endswith(".tsv") or url.endswith(".tsv.gz") or url.endswith(".tsv.zip")):
        ext_type = "tsv"
    else:
        utils.warn("Unknown file extension. Doing best effort in content type detection")

    # read response
    response_str, status_code, error_msg, = read_url_response(url, query_params, headers, body = None, username = username, password = password, api_token = api_token,
        timeout_sec = timeout_sec, verify = verify, method = method)

    # check for status code
    if (status_code != 200):
        raise Exception("read_url failed. Status: {}, Reason: {}".format(status_code, error_msg))

    # split into lines
    lines = response_str.split("\n")
    header = lines[0]
    data = lines[1:]

    # check for separator
    if (sep is None and "\t" not in response_str):
        if ("," in header or ext_type == "csv"):
            sep = ","

    # check for other separators
    if (sep is not None and sep != "\t"):
        # do a better version of separator detection
        if ("\t" in response_str):
            utils.warn("Non TSV input file has tabs. Converting tabs to spaces")
            header = header.replace("\t", " ")
            data = [x.replace("\t", " ") for x in data]

        # replace separator with tabs
        header = header.replace(sep, "\t")
        data = [x.replace(sep, "\t") for x in data]

    # return
    return tsv.TSV(header, data).validate()

