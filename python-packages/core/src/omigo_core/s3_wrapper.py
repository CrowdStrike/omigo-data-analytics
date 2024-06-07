"""wrapper methods to work with S3"""
import boto3
import gzip
import os
import zipfile
from io import BytesIO

# local import
from omigo_core import utils
import threading
import traceback

# define some global variables for caching s3 bucket and session
S3_RESOURCE = {}
S3_SESSIONS = {}
S3_BUCKETS = {}
S3_CLIENTS = {}
S3_RESOURCE_LOCK = threading.Lock()
S3_SESSION_LOCK = threading.Lock()
S3_CLIENT_LOCK = threading.Lock()

def create_session_key(s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    if (s3_region is None and aws_profile is None):
        return "DEFAULT_KEY"
    else:
        s3_region_str = "" if (s3_region is None) else s3_region
        aws_profile_str = "" if (aws_profile is None) else aws_profile 
        return "{}:{}".format(s3_region_str, aws_profile_str)

def get_s3_session(s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)

    # show the s3 settings
    # utils.info_once("get_s3_session: s3_region: {}, aws_profile: {}".format(s3_region, aws_profile))

    # generate s3_session
    if (s3_region is not None and aws_profile is not None):
        session = boto3.session.Session(region_name = s3_region, profile_name = aws_profile)
        # utils.debug("get_s3_session: s3_region: {}, aws_profile: {}, session: {}".format(s3_region, aws_profile, session))
    else:
        session = boto3.session.Session(region_name = s3_region, profile_name = aws_profile)
        # utils.debug("get_s3_session: no s3_region or aws_profile, session: {}".format(session))

    # return
    return session

def get_s3_session_cache(s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    key = create_session_key(s3_region, aws_profile)

    # make it thread safe
    with S3_SESSION_LOCK:
        if ((key in S3_SESSIONS.keys()) == False):
            S3_SESSIONS[key] = get_s3_session(s3_region, aws_profile)

    utils.debug("get_s3_session_cache: s3_region: {}, aws_profile: {}, key: {}, sessions: {}".format(s3_region, aws_profile, key, S3_SESSIONS))
    return S3_SESSIONS[key]

# TODO
def get_s3_resource(s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    session = get_s3_session_cache(s3_region, aws_profile)

    return session.resource("s3")

def get_s3_resource_cache(s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    key = create_session_key(s3_region, aws_profile)

    # make it thread safe
    with S3_RESOURCE_LOCK:
        if ((key in S3_RESOURCE.keys()) == False):
            S3_RESOURCE[key] = get_s3_resource(s3_region, aws_profile)

    # return
    return S3_RESOURCE[key]

def get_s3_client(s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    session = get_s3_session_cache(s3_region, aws_profile)
    return session.client("s3")

def get_s3_client_cache(s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    key = create_session_key(s3_region, aws_profile)

    # make it thread safe
    with S3_CLIENT_LOCK:
        if ((key in S3_CLIENTS.keys()) == False):
            S3_CLIENTS[key] = get_s3_client(s3_region, aws_profile)

    # return
    return S3_CLIENTS[key]

def get_s3_bucket(bucket_name, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    s3 = get_s3_resource_cache(s3_region, aws_profile)

    return s3.Bucket(bucket_name)

def get_s3_bucket_cache(bucket_name, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    if ((bucket_name in S3_BUCKETS.keys()) == False):
        S3_BUCKETS[bucket_name] = get_s3_bucket(bucket_name, s3_region, aws_profile)

    # return
    return S3_BUCKETS[bucket_name]

def get_file_content(bucket_name, object_key, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    s3_bucket = get_s3_bucket_cache(bucket_name, s3_region, aws_profile)
    obj = s3_bucket.Object(object_key)
    obj_content = obj.get()
    body = obj_content["Body"]
    data = body.read()
    body.close()

    return data

# TODO: Deprecated
def get_s3_file_content(bucket_name, object_key, s3_region = None, aws_profile = None):
    utils.warn_once("use get_file_content instead")
    return get_file_content(bucket_name, object_key, s3_region = s3_region, aws_profile = aws_profile)

# TODO: Deprecated
def get_s3_file_content_as_text(bucket_name, object_key, s3_region = None, aws_profile = None):
    utils.warn_once("use get_file_content_as_text instead")
    return get_file_content_as_text(bucket_name, object_key, s3_region = s3_region, aws_profile = aws_profile)

def get_file_content_as_text(bucket_name, object_key, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    barr = get_file_content(bucket_name, object_key, s3_region, aws_profile)

    # check for gz or zip
    if (object_key.endswith(".gz")):
        barr = gzip.decompress(barr)
    elif(object_key.endswith(".zip")):
        # get only the filename
        zfile = zipfile.ZipFile(BytesIO(barr))
        barr = zfile.open(zfile.infolist()[0]).read()
        zfile.close()

    # return bytearray
    barr = bytearray(barr)
    return barr.decode().rstrip("\n")

# TODO: this is expensive and works in specific scenarios only especially for files
def check_path_exists(path, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)

    s3 = get_s3_client_cache(s3_region = s3_region, aws_profile = aws_profile)
    bucket_name, object_key = utils.split_s3_path(path)

    # show the s3 settings
    utils.info_once("check_path_exists: s3_region: {}, aws_profile: {}".format(s3_region, aws_profile))
    utils.info_once("check_path_exists: s3_region: {}, aws_profile: {}, s3: {}".format(s3_region, aws_profile, s3))

    results = s3.list_objects(Bucket = bucket_name, Prefix = object_key)
    return "Contents" in results

def check_file_exists(path, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    s3 = get_s3_client_cache(s3_region = s3_region, aws_profile = aws_profile)
    bucket_name, object_key = utils.split_s3_path(path)
    try:
        # call head_object which is supposed to be efficient. This works only for files and not directories?
        response = s3.head_object(Bucket = bucket_name, Key = object_key)

        # check response
        if (response is not None):
            return True
        else:
            return False
    except Exception as e:
        utils.error("check_file_exists: path: {}, s3_region: {}, aws_profile: {}, exception: {}".format(path, s3_region, aws_profile, e))
        utils.error("check_file_exists: StackTrace: {}".format(traceback.format_exc()))
        return False

def put_file_content(bucket_name, object_key, barr, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    s3_bucket = get_s3_resource_cache(s3_region, aws_profile)
    obj = s3_bucket.Object(bucket_name, object_key)
    obj.put(Body = barr)

# TODO: Deprecated
def put_s3_file_content(bucket_name, object_key, barr, s3_region = None, aws_profile = None):
    utils.warn_once("use put_file_content instead")
    return put_file_content(bucket_name, object_key, barr, s3_region = None, aws_profile = None)

def put_file_with_text_content(bucket_name, object_key, text, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    barr = str.encode(text)
    if (object_key.endswith(".gz")):
        barr = gzip.compress(barr)
    elif (object_key.endswith(".zip")):
        # get only the filename
        object_file_name = object_key.split("/")[-1]
        mzip = BytesIO()
        with zipfile.ZipFile(mzip, mode = "w", compression = zipfile.ZIP_DEFLATED) as zfile:
            zfile.writestr(object_file_name[0:-4], barr)
        barr = mzip.getvalue()

    put_file_content(bucket_name, object_key, barr, s3_region = s3_region, aws_profile = aws_profile)

# TODO: Deprecated
def put_s3_file_with_text_content(bucket_name, object_key, text, s3_region = None, aws_profile = None):
    utils.warn_once("use put_file_with_text_content instead")
    put_file_with_text_content(bucket_name, object_key, text, s3_region = s3_region, aws_profile = aws_profile)
 
def resolve_region_profile(s3_region = None, aws_profile = None):
    # resolve s3_region
    if ((s3_region is None or s3_region == "") and "S3_REGION" in os.environ.keys()):
        s3_region = os.environ["S3_REGION"]

    # resolve aws_profile
    if ((aws_profile is None or aws_profile == "") and "AWS_PROFILE" in os.environ.keys()):
        aws_profile = os.environ["AWS_PROFILE"]

    # return
    return s3_region, aws_profile

# https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2
def __get_all_s3_objects__(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')

# FIXME: This method is implemented using reverse engineering. Not so reliable
# TODO: ignore_if_missing should be FALSE by default
def get_directory_listing(path, filter_func = None, ignore_if_missing = False, skip_exist_check = False, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    s3 = get_s3_client_cache(s3_region = s3_region, aws_profile = aws_profile)

    # split the path
    bucket_name, object_key = utils.split_s3_path(path)

    filenames = []
    # validation
    if (check_path_exists(path, s3_region = s3_region, aws_profile = aws_profile) == False):
        if (ignore_if_missing == False):
            raise Exception("Directory does not exist: " + path)
        else:
            utils.debug("Directory does not exist: {}".format(path))
            return []

    # set the max keys as boto3 api are weird
    response = __get_all_s3_objects__(s3, Bucket = bucket_name, Prefix = object_key)

    # iterate through response
    count = 0
    for dir_content in response:
        count = count + 1
        # utils.debug("Response: {}".format(dir_content))
        filename = dir_content['Key']

        # check if the directory listing was done on leaf node in which case ignore
        if (object_key == filename):
            continue

        # extract the key for remaining
        extracted_key = filename[len(object_key) + 1:]
        # utils.debug("extracted_key: {}".format(extracted_key))

        # this had directory as child
        parts = extracted_key.split("/")
        base_filename = None
        if (len(parts) == 1):
            # immediate child is a leaf node
            base_filename = parts[0]
        else:
            # immediate child is a directory
            base_filename = parts[0]

        # debug
        # utils.debug("object_key: {}, filename: {}, extracted_key: {}, base_filename: {}".format(object_key, filename, extracted_key, base_filename))

        # collect all the paths found
        if (base_filename is not None):
            # extract the last part and then prepend the bucket and object key to construct full path
            filenames.append(path + "/" + base_filename)

    # print the number of object_keys found
    utils.trace("Number of entries found in directory listing: {}: {}".format(path, count))

    # dedup
    filenames = sorted(list(set(filenames)))

    # valid files as boto does a prefix search
    if (skip_exist_check == False):
        filenames = list(filter(lambda t: check_path_exists(t, s3_region = s3_region, aws_profile = aws_profile), filenames))

    # apply filter func if any
    if (filter_func is not None):
        filenames = list(filter(lambda t: filter_func(t), filenames))

    # return
    return filenames

def delete_file(path, ignore_if_missing = False, s3_region = None, aws_profile = None):
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    s3 = get_s3_client_cache(s3_region = s3_region, aws_profile = aws_profile)

    # split the path
    bucket_name, object_key = utils.split_s3_path(path)

    # check if the file exists
    if (check_path_exists(path, s3_region = s3_region, aws_profile = aws_profile) == False):
        if (ignore_if_missing == False):
            raise Exception("delete_file: path doesnt exist: {}".format(path))
        else:
            utils.debug("delete_file: path doesnt exist: {}".format(path))

        return

    # delete
    s3.delete_object(Bucket = bucket_name, Key = object_key)

def get_last_modified_time(path, ignore_if_missing = False, s3_region = None, aws_profile = None):
    # check if exists
    if (check_path_exists(path, s3_region = s3_region, aws_profile = aws_profile) == False):
        if (ignore_if_missing == False):
            raise Exception("get_last_modified_time: path doesnt exist: {}".format(path))
        else:
            utils.debug("get_last_modified_time: path doesnt exist: {}".format(path))

        return None

    # parse
    s3_region, aws_profile = resolve_region_profile(s3_region, aws_profile)
    s3 = get_s3_client_cache(s3_region = s3_region, aws_profile = aws_profile)

    # split the path
    bucket_name, object_key = utils.split_s3_path(path)

    # call head_object to get metadata
    response = s3.head_object(Bucket = bucket_name, Key = object_key)

    # validation
    if (response is not None):
        # get datetime
        datetime_value = response["LastModified"]

        # return
        return datetime_value
    else:
        return None
