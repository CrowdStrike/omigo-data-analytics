"""wrapper methods to work with S3"""
import boto3
import gzip
import os
import zipfile
from io import BytesIO

# local import
from omigo_core import utils
import threading

# define some global variables for caching s3 bucket and session
S3_RESOURCE = {}
S3_SESSIONS = {}
S3_BUCKETS = {}
S3_CLIENTS = {}
S3_RESOURCE_LOCK = threading.Lock()
S3_SESSION_LOCK = threading.Lock()
S3_CLIENT_LOCK = threading.Lock()
S3_DEFAULT_REGION = "us-west-1"
S3_DEFAULT_PROFILE = "default"
#S3_WARNING_GIVEN = "0" 

def create_session_key(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    if (region is None and profile is None):
        return "DEFAULT_KEY"
    else:
        return region + ":" + profile

def get_s3_session(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)

    # generate s3_session
    if (region is not None and profile is not None):
        utils.info("get_s3_session: region: {}, profile: {}".format(region, profile))
        session = boto3.session.Session(region_name = region, profile_name = profile)
    else:
        utils.info("get_s3_session: no region or profile")
        session = boto3.session.Session()

    # return
    return session

def get_s3_session_cache(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    key = create_session_key(region, profile)
    
    # make it thread safe
    with S3_SESSION_LOCK:
        if ((key in S3_SESSIONS.keys()) == False):
            S3_SESSIONS[key] = get_s3_session(region, profile)

    return S3_SESSIONS[key]

# TODO
def get_s3_resource(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    session = get_s3_session_cache(region, profile)

    return session.resource("s3")

def get_s3_resource_cache(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    key = create_session_key(region, profile)

    # make it thread safe
    with S3_RESOURCE_LOCK:
        if ((key in S3_RESOURCE.keys()) == False):
            S3_RESOURCE[key] = get_s3_resource(region, profile)

    return S3_RESOURCE[key]

def get_s3_client(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    session = get_s3_session_cache(region, profile)

    return session.client("s3")

def get_s3_client_cache(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    key = create_session_key(region, profile)

    # make it thread safe
    with S3_CLIENT_LOCK:
        if ((key in S3_CLIENTS.keys()) == False):
            S3_CLIENTS[key] = get_s3_client(region, profile)

    return S3_CLIENTS[key]

def get_s3_bucket(bucket_name, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3 = get_s3_resource_cache(region, profile)

    return s3.Bucket(bucket_name)

def get_s3_bucket_cache(bucket_name, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    if ((bucket_name in S3_BUCKETS.keys()) == False):
        S3_BUCKETS[bucket_name] = get_s3_bucket(bucket_name, region, profile)

    return S3_BUCKETS[bucket_name]

def get_s3_file_content(bucket_name, object_key, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3_bucket = get_s3_bucket_cache(bucket_name, region, profile)
    obj = s3_bucket.Object(object_key)
    obj_content = obj.get()
    body = obj_content["Body"]
    data = body.read()
    body.close()

    return data

def get_s3_file_content_as_text(bucket_name, object_key, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    barr = get_s3_file_content(bucket_name, object_key, region, profile)
    if (object_key.endswith(".gz")):
        barr = gzip.decompress(barr)
    elif(object_key.endswith(".zip")):
        # get only the filename
        zfile = zipfile.ZipFile(BytesIO(barr))
        barr = zfile.open(zfile.infolist()[0]).read()
        zfile.close()

    barr = bytearray(barr)
    return barr.decode().rstrip("\n")

# TODO: this is expensive and works in specific scenarios only especially for files
def check_path_exists(path, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3 = get_s3_client_cache(region, profile)
    bucket_name, object_key = utils.split_s3_path(path)

    results = s3.list_objects(Bucket = bucket_name, Prefix = object_key)
    return "Contents" in results

def check_file_exists(path, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3 = get_s3_client_cache(region, profile)
    bucket_name, object_key = utils.split_s3_path(path)
    try:
        # call head_object which is supposed to be efficient. This works only for files and not directories?
        response = s3.head_object(Bucket = bucket_name, Key = object_key)

        # check response
        if (response is not None):
            return True
        else:
            return False
    except:
        return False

def put_s3_file_content(bucket_name, object_key, barr, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3_bucket = get_s3_resource_cache(region, profile)
    obj = s3_bucket.Object(bucket_name, object_key)
    obj.put(Body = barr)
   
def put_s3_file_with_text_content(bucket_name, object_key, text, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
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

    put_s3_file_content(bucket_name, object_key, barr, region, profile)

def resolve_region_profile(region = None, profile = None):
    # resolve region
    if ((region == "" or region is None) and "S3_REGION" in os.environ.keys()):
        region = os.environ["S3_REGION"]

    # resolve profile
    if ((profile == "" or profile is None) and "AWS_PROFILE" in os.environ.keys()):
        profile = os.environ["AWS_PROFILE"]

    # return
    return region, profile

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
def get_directory_listing(path, filter_func = None, fail_if_missing = True, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3 = get_s3_client_cache(region, profile)

    # split the path
    bucket_name, object_key = utils.split_s3_path(path)

    filenames = []
    # validation
    if (check_path_exists(path, region, profile) == False):
        if (fail_if_missing):
            raise Exception("Directory does not exist: " + path)
        else:
            if (utils.is_debug()):
                print("Directory does not exist: " + path)
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
    filenames = list(filter(lambda t: check_path_exists(t), filenames))

    # apply filter func if any
    if (filter_func is not None):
        filenames = list(filter(lambda t: filter_func(t), filenames))

    # return
    return filenames

def delete_file(path, fail_if_missing = False, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3 = get_s3_client_cache(region, profile)

    # split the path
    bucket_name, object_key = utils.split_s3_path(path)

    # check if the file exists
    if (check_path_exists(path) == False):
        if (fail_if_missing):
            raise Exception("delete_file: path doesnt exist: {}".format(path))
        else:
            utils.debug("delete_file: path doesnt exist: {}".format(path))

        return

    # delete
    s3.delete_object(Bucket = bucket_name, Key = object_key)

def get_last_modified_time(path, fail_if_missing = False, region = None, profile = None):
    # check if exists
    if (check_path_exists(path) == False):
        if (fail_if_missing):
            raise Exception("get_last_modified_time: path doesnt exist: {}".format(path))
        else:
            utils.debug("get_last_modified_time: path doesnt exist: {}".format(path))

        return None 

    # parse
    region, profile = resolve_region_profile(region, profile)
    s3 = get_s3_client_cache(region, profile)

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
