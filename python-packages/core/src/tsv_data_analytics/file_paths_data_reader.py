"""FilePathsDataReader class"""

import sys

# local import
from tsv_data_analytics import file_paths_reader
from tsv_data_analytics import file_paths_util

class FilePathsDataReader:
    """A simple class to read filepaths data"""

    s3_region = ""
    aws_profile = ""  
 
    # file paths reader is an iterator based reader 
    file_paths_readers = None 

    # cur_data holds the current data block
    cur_data = None

    # cur_index is the pointer to the line in cur_data
    cur_index = 0
 
    # header is the header line for entire dataset
    header = None
   
    # constructor 
    def __init__(self, filepaths, s3_region, aws_profile):
        # s3 config
        self.s3_region = s3_region
        self.aws_profile = aws_profile

        # initialize the reader
        self.file_paths_readers = file_paths_reader.FilePathsReader(filepaths)

        # loop through all files until find a one with proper data
        found = False
        while (found == False):
            if (self.file_paths_readers.has_next()):
                # initial load
                filepath = self.file_paths_readers.next()
                self.cur_data = file_paths_util.read_file_content_as_lines(filepath, self.s3_region, self.aws_profile)

                # check for validity
                if (len(self.cur_data) == 0):
                    print("FilePathsDataReader: Invalid file found. No header.", filepath)
                    sys.exit(0)

                # read header
                self.header = self.cur_data[0].rstrip("\n")

                # assign cur_index to 0 for reading header
                self.cur_index = 1

                # check if found a valid file
                if (self.cur_index < len(self.cur_data)):
                    found = True
            else:
                self.cur_data = None
                self.cur_index = -1
                break

        # check if found some valid file to read
        if (found == False):
            self.cur_data = None
            self.cur_index = -1

    # get header
    def get_header(self):
        return self.header

    # has next returns boolean if there is still some data left
    def has_next(self):
        # check if no data block loaded
        if (self.cur_data is None):
            return False

        return True

    # close the file reader
    def close(self):
        self.cur_data = None
        self.cur_index = -1

    # next return the next line and also moves the pointers
    def next(self):
        # declare and initialize variable
        result = None

        # cur_data should be non empty
        if (self.has_next() == False):
            print("FilePathsDataReader: next() has next is false and next is called")
            return None
        
        # check for current pointer
        if (self.cur_index < len(self.cur_data)):
            result = self.cur_data[self.cur_index].rstrip("\n")
            self.cur_index = self.cur_index + 1
        else:
            print("FilePathsDataReader: next() this should not happen")

        # Check if cur_index has reached the end of data block
        if (self.cur_index >= len(self.cur_data)):
            # check if any more data blocks are left
            if (self.file_paths_readers.has_next() == False):
                self.cur_data = None
                self.cur_index = -1
            else:
                found = False
                while (found == False):
                    if (self.file_paths_readers.has_next()):
                        # read the next block and reinitialize the cur_index
                        filepath = self.file_paths_readers.next()
                        self.cur_data = file_paths_util.read_file_content_as_lines(filepath, self.s3_region, self.aws_profile)
                        self.cur_index = 1

                        # check if found a valid file
                        if (self.cur_index < len(self.cur_data)):
                            found = True 
                    else:
                        # invalidate the buffers
                        self.cur_data = None
                        self.cur_index = -1
                        break

        # return result
        return result

