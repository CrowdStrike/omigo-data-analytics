"""FileReader / FileWriter class"""

import gzip
import zipfile

from omigo_core import s3_wrapper
from omigo_core import utils

class FileWriter:
    """FileWriter class to write data into files"""
    def __init__(self, output_file_name, s3_region, aws_profile):
        self.output_file_name = output_file_name
        self.data = []
        self.s3_region = s3_region
        self.aws_profile = aws_profile

    def write(self, line):
        self.data.append(line)

    def close(self):
        output_zipf = None
        output_file = None
        if (self.output_file_name.startswith("s3://")):
            content = "".join(self.data)
            bucket_name, object_key = utils.split_s3_path(self.output_file_name)
            s3_wrapper.put_s3_file_with_text_content(bucket_name, object_key, content, self.s3_region, self.aws_profile)
        else:
            # construct output file
            if (self.output_file_name.endswith(".gz")):
                output_file = gzip.open(self.output_file_name, "wt")
                # write all the content
                for line in self.data:
                    output_file.write(line) 
            elif (self.output_file_name.endswith(".zip")):
                output_zipf = zipfile.ZipFile(self.output_file_name, "w")
                output_file = output_zipf.open(self.output_file_name.split("/")[-1][0:-4], "w")
                # write all the content
                for line in self.data:
                    output_file.write(str.encode(line))
            else:
                output_file = open(self.output_file_name, "w")
                # write all the content
                for line in self.data:
                    output_file.write(line) 

            # close
            if (output_file is not None):
                output_file.close()
            if (output_zipf is not None):
                output_zipf.close()

            # set data to None
            self.data = None

class FileReader:
    """FileReader class to read data files"""
    def __init__(self, input_file_name, s3_region, aws_profile):
        pass

    def read(self):
        pass

    def close(self):
        pass
        

