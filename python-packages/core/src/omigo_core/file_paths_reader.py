"""FilePathsReader class"""

class FilePathsReader:
    """A simple class to read filepaths data"""

    # filepaths is the array of paths 
    filepaths = None 

    # cur_index is the pointer to the current index in filepaths
    cur_index = -1
    
    # constructor
    def __init__(self, filepaths):
        self.cur_index = 0
        self.filepaths = filepaths

    # has next returns true if there is still data to be read
    def has_next(self):
       return self.filepaths is not None and self.cur_index < len(self.filepaths)

    # close the file reader
    def close(self):
        self.cur_data = len(self.filepaths) + 1 

    # next returns the next data. None otherwise
    def next(self):
       if (self.has_next() == False):
           return None

       # advance the pointer for next time
       result = self.filepaths[self.cur_index]
       self.cur_index = self.cur_index + 1
       
       # check for end
       if (self.cur_index >= len(self.filepaths)):
           self.filepaths = None
           self.cur_index = -1

       # print which file is processed
       print("FilePathsReader: processing file: ", result)
       return result


