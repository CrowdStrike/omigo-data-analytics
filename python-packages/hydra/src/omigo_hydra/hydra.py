from omigo_core import file_paths_util, file_io_wrapper, utils

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
