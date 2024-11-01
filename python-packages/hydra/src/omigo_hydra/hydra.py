from omigo_core import utils, dataframe
from omigo_hydra import file_paths_data_reader, file_paths_util, s3io_wrapper

def save_to_file(xtsv, output_file_name, s3_region = None, aws_profile = None):
    # do some validation
    xtsv = xtsv.validate()

    # check if it is a local file or s3
    if (output_file_name.startswith("s3://") == False):
        file_paths_util.create_local_parent_dir(output_file_name)

    # construct output file
    output_file = file_io_wrapper.TSVFileWriter(s3_region, aws_profile)
    
    # write
    output_file.save(xtsv, output_file_name)
    
    # debug
    utils.debug("save_to_file: file saved to: {}, num_rows: {}, num_cols: {}".format(output_file_name, xtsv.num_rows(), xtsv.num_cols()))

def check_exists(xtsv, s3_region = None, aws_profile = None):
    return file_paths_util.check_exists(xtsv, s3_region, aws_profile)

def read(input_file_or_files, sep = None, def_val_map = None, username = None, password = None, num_par = 0, s3_region = None, aws_profile = None):
    # convert the input to array
    input_files = utils.get_argument_as_array(input_file_or_files)

    # tasks 
    tasks = []

    def __read_inner__(input_file):
        # read file content                               
        lines = file_paths_util.read_file_content_as_lines(input_file, s3_region, aws_profile)
                                                          
        # take header and dat
        header = lines[0]
        data = lines[1:]
            
        # check if a custom separator is defined
        if (sep is not None):
            # check for validation
            for line in lines:
                if ("\t" in line):
                    raise Exception("Cant parse non tab separated file as it contains tab character:", input_file)

            # create header and data
            header = header.replace(sep, "\t")
            data = [x.replace(sep, "\t") for x in data]

        # return
        return tsv.TSV(header, data)

    # create tasks
    for input_file in input_files:
        # check if it is a file or url
        if (input_file.startswith("http")):
            tsv_list.append(read_url_as_tsv(input_file, username = username, password = password))
            tasks.append(utils.ThreadPoolTask(read_url_as_tsv, input_file, username = username, password = password))
        else:
            tasks.append(utils.ThreadPoolTask(__read_inner__, input_file))

    # get result
    tsv_list = utils.run_with_thread_pool(tasks, num_par = num_par, wait_sec = 1)

    # merge and return
    return merge(tsv_list, def_val_map = def_val_map)

def __read_with_filter_transform_select_func__(cols):
    # create a inner function
    def __read_with_filter_transform_select_func_inner__(mp):
        result_mp = {}
        for c in cols:
            if (c in mp.keys()):
                result_mp[c] = str(mp[c])

        # return
        return result_mp

    return __read_with_filter_transform_select_func_inner__

def read_with_filter_transform(input_file_or_files, sep = None, def_val_map = None, filter_transform_func = None, cols = None, transform_func = None, s3_region = None, aws_profile = None):
    # check if cols is defined
    if (filter_transform_func is not None and cols is not None):
        raise Exception("tsvutils: read_with_filter_transform: either of filter_transform_func or cols parameter can be used")

    # use the map function for cols
    if (cols is not None and len(cols) > 0):
        filter_transform_func = __read_with_filter_transform_select_func__(cols)

    # check if filter_transform_func is defined
    if (filter_transform_func is None):
        xtsv = read(input_file_or_files, sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)

        # apply transform_func if defined
        xtsv_transform = transform_func(xtsv) if (transform_func is not None) else xtsv

        # return
        return xtsv_transform
    else:
        # resolve input
        input_files = utils.get_argument_as_array(input_file_or_files)

        # initialize result
        tsv_list = []

        # common keys
        common_keys = {}

        # iterate over all input files
        for input_file in input_files:
            # read the file
            x = read(input_file, sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)

            # update the common
            for h in x.get_header_fields():
                if (h not in common_keys.keys()):
                    common_keys[h] = 0
                common_keys[h] = common_keys[h] + 1

            # gather maps of maps
            result_maps = []
            keys = {}

            # iterate over the records of each map and generate a new one
            for mp in x.to_maps():
                mp2 = filter_transform_func(mp)
                if (mp2 is not None):
                    if (len(mp2) > 0):
                        result_maps.append(mp2)
                    for k in mp2.keys():
                        keys[k] = 1

            # check for empty maps
            if (len(keys) > 0):
                # output keys
                keys_sorted = []
                first_file = read(input_files[0], sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)
                for h in first_file.get_header_fields():
                    if (h in keys.keys()):
                        keys_sorted.append(h)

                # new header and data
                header2 = "\t".join(keys_sorted)
                data2 = []

                # iterate and generate header and data
                for mp in result_maps:
                    fields = []
                    for k in keys_sorted:
                        fields.append(mp[k])
                    data2.append("\t".join(fields))

                # debugging
                utils.trace("tsvutils: read_with_filter_transform: file read: {}, after filter num_rows: {}".format(input_file, len(data2)))

                # result tsv
                xtsv = tsv.TSV(header2, data2)

                # apply transformation function if defined
                xtsv_transform = transform_func(xtsv) if (transform_func is not None) else xtsv
                tsv_list.append(xtsv_transform)

        # Do a final check to see if all tsvs are empty
        if (len(tsv_list) > 0):
            # call merge on tsv_list
            return merge(tsv_list)
        else:
            # create an empty data tsv file with common header fields
            header_fields = []
            first_file = read(input_files[0], sep = sep, def_val_map = def_val_map, s3_region = s3_region, aws_profile = aws_profile)
            for h in first_file.get_header_fields():
                if (common_keys[h] == len(input_files)):
                    header_fields.append(h)

            new_header = "\t".join(header_fields)
            new_data = []

            # return
            return tsv.TSV(new_header, new_data)

# TODO: replace this by etl_ext
def read_by_date_range(path, start_date_str, end_date_str, prefix, s3_region = None, aws_profile = None, granularity = "daily"):
    utils.warn_once("read_by_date_range: probably Deprecated")
    # read filepaths
    filepaths = file_paths_util.read_filepaths(path, start_date_str, end_date_str, prefix, s3_region, aws_profile, granularity)

    # check for headers validity
    if (file_paths_util.has_same_headers(filepaths, s3_region, aws_profile) == False):
        utils.warn("Mismatch in headers for different days. Choose the right date range: start: {}, end: {}".format(start_date_str, end_date_str))
        return None

    # read individual tsvs
    tsv_list = []
    for filepath in filepaths:
        x = read(filepath)
        tsv_list.append(x)

    # combine all together
    if (len(tsv_list) == 0):
        return None
    elif (len(tsv_list) == 1):
        return tsv_list[0]
    else:
        return tsv_list[0].union(tsv_list[1:])

def load_from_dir(path, start_date_str, end_date_str, prefix, s3_region = None, aws_profile = None, granularity = "daily"):
    # read filepaths
    filepaths = file_paths_util.read_filepaths(path, start_date_str, end_date_str, prefix, s3_region, aws_profile, granularity)
    return load_from_files(filepaths, s3_region, aws_profile)

def load_from_files(filepaths, s3_region, aws_profile):
    # check for headers validity
    if (file_paths_util.has_same_headers(filepaths, s3_region, aws_profile) == False):
        print("Invalid headers.")
        return None

    # initialize the file reader
    file_reader = file_paths_data_reader.FilePathsDataReader(filepaths, s3_region, aws_profile)

    # get header
    header = file_reader.get_header()
    data = []

    # get data
    while file_reader.has_next():
        # read next record
        line = file_reader.next()
        data.append(line)

    # close
    file_reader.close()

    return tsv.TSV(header, data)

def read_json_files_from_directories_as_tsv(paths, s3_region = None, aws_profile = None):
    # initialize fs
    fs = s3io_wrapper.S3FSWrapper(s3_region = s3_region, aws_profile = aws_profile)

    # result
    result = []

    # iterate through each directory
    for path in paths:
        # list all files
        files = fs.list_leaf_dir(path)

        # read file as set of lines
        for f in files:
            full_path = "{}/{}".format(path, f)
            lines = fs.read_file_contents_as_text(full_path).split("\n")

            # append to result
            result = result + lines

    # remove empty lines
    result = list(filter(lambda t: t.strip() != "", result))

    # create dataframe
    header_fields = ["json"]
    data_fields = list([t.split("\t") for t in result])
    df = dataframe.new_with_cols(header_fields, data = data_fields)

    # return
    return df

