package omigo_core

class FilePathsReader(val filePaths: List[String]) {
  var curIndex = 0

  def hasNext() = {
    // TODO: changing the logic. Need to reflect in python
    curIndex != -1 && curIndex < filePaths.length
  }

  def close(): Unit = {
    // TODO: python code has issue
  }

  def next(): String = {
    if (hasNext() == false)
      return null 

    // advance the pointer for next time
    val result = filePaths(curIndex)
    curIndex = curIndex + 1

    // check for end
    if (curIndex >= filePaths.length) {
      // TODO : no need to do this
      // filepaths = None
      curIndex = -1
    }

    // print which file is processed
    println("FilePathsReader: processing file: " + result)
    result
  } 
}
