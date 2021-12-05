"""wrapper methods to work with S3"""
import boto3
import gzip
import os
import zipfile
from io import BytesIO

# local import
from tsv_data_analytics import utils

# define some global variables for caching s3 bucket and session
S3_RESOURCE = {}
S3_SESSIONS = {}
S3_BUCKETS = {}
S3_CLIENTS = {}

S3_DEFAULT_REGION = "us-west-1"
S3_DEFAULT_PROFILE = "default"
#S3_WARNING_GIVEN = "0" 

def create_session_key(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    return region + ":" + profile

def get_s3_session(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    session = boto3.session.Session(region_name = region, profile_name = profile)

    return session

def get_s3_session_cache(region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    key = create_session_key(region, profile)
    
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

def check_path_exists(path, region = None, profile = None):
    region, profile = resolve_region_profile(region, profile)
    s3 = get_s3_client_cache(region, profile)
    bucket_name, object_key = utils.split_s3_path(path)
    results = s3.list_objects(Bucket = bucket_name, Prefix = object_key)
    return "Contents" in results 

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
    if (region == "" or region is None):
        region = os.getenv("S3_REGION")

    if (profile == "" or profile is None):
        profile = os.getenv("AWS_PROFILE")

    if (region == "" or region is None):
        # warn only once
        #if (S3_WARNING_GIVEN == "0"):
        #    print ("[WARN] S3 region not defined. Please set environment variable S3_REGION and AWS_PROFILE. Using 'us-west-1'")
        #    S3_WARNING_GIVEN = "1" 
        region = S3_DEFAULT_REGION

    if (profile == "" or profile is None):
        # warn only once
        #if (S3_WARNING_GIVEN == "0"):
        #    print ("[WARN] AWS Profile not defined. Please set environment variable S3_REGION and AWS_PROFILE. Using 'default'")
        #    S3_WARNING_GIVEN = "1" 
        profile = S3_DEFAULT_PROFILE

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
        # utils.debug("Response: {}".format(response))
        filename = dir_content['Key']

        # check if the directory listing was done on leaf node in which case ignore
        if (object_key != filename):
            extracted_key = filename[len(object_key) + 1:]
            # this had directory as child
            parts = extracted_key.split("/")
            base_filename = None
            if (len(parts) == 1):
                # immediate child is a leaf node
                base_filename = parts[0]
            elif (len(parts) == 2):
                # immediate child is a directory
                base_filename = parts[0]

            # debug
            utils.trace("object_key: {}, filename: {}, extracted_key: {}, base_filename: {}".format(object_key, filename, extracted_key, base_filename))

            # collect all the paths found
            if (base_filename != None):
                # extract the last part and then prepend the bucket and object key to construct full path
                filenames.append(path + "/" + base_filename)

    # print the number of object_keys found
    utils.trace("Number of entries found in directory listing: {}: {}".format(path, count))

    # dedup
    filenames = sorted(list(set(filenames)))
    if (filter_func != None):
        filenames = list(filter(lambda t: filter_func(t), filenames))

    # return
    return filenames

