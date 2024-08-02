package omigo_core

class FilePathsDataReader(val filePaths: List[String], val s3Region: String, val awsProfile: String) {
  var filePathsReaders = new FilePathsReader(filePaths) 
  var curData: List[String] = null
  var curIndex: Int = 0
  var header: String = null

  def apply() {
    var found = false
    var flag = true
    while (flag == true && found == false) {
      if (filePathsReaders.hasNext()) {
        // initial load
        val filePath = filePathsReaders.next()
        curData = FilePathsUtil.read_file_content_as_lines(filePath, s3Region, awsProfile)

        // check for validity
        if (curData.length == 0) {
          // TODO: change this in python
          throw new Exception("FilePathsDataReader: Invalid file found. No header." + filePath)
        } 
        
        // read header
        header = curData(0).stripSuffix("\n")

        // assign cur_index to 0 for reading header
        curIndex = 1

        // check if found a valid file
        if (curIndex < curData.length)
          found = true
      } else {
        curData = null 
        curIndex = -1
        flag = false
      }
    }

    // check if found some valid file to read
    if (found == false) {
      curData = null 
      curIndex = -1
    }
  }

  // get header
  def getHeader() = {
      header
  }

  // has next returns boolean if there is still some data left
  def hasNext() = {
    // check if no data block loaded
    curData != null
  }

  // close the file reader
  def close(): Unit = {
    curData = null 
    curIndex = -1
  }

  // next return the next line and also moves the pointers
  def next(): String = {
    // declare and initialize variable
    var result: String = null 

    // cur_data should be non empty
    if (hasNext() == false) {
        // TODO: change python code
        throw new Exception("FilePathsDataReader: next() has next is false and next is called")
    }

    // check for current pointer
    if (curIndex < curData.length) {
        result = curData(curIndex).stripSuffix("\n")
        curIndex = curIndex + 1
    } else {
        // TODO: change python code
        throw new Exception("FilePathsDataReader: next() this should not happen")
    }

    // Check if cur_index has reached the end of data block
    if (curIndex >= curData.length) {
      // check if any more data blocks are left
      if (filePathsReaders.hasNext() == false) {
        curData = null
        curIndex = -1
      } else {
        var found = false
        var flag = true 
        while (flag == true && found == false) {
          if (filePathsReaders.hasNext()) {
            // read the next block and reinitialize the cur_index
            val filePath = filePathsReaders.next()
            curData = FilePathsUtil.read_file_content_as_lines(filePath, s3Region, awsProfile)
            curIndex = 1

            // check if found a valid file
            if (curIndex < curData.length)
              found = true
          } else {
            // invalidate the buffers
            curData = null
            curIndex = -1
            flag = false
          }
        }
      }
    }

    return result
  }
}
