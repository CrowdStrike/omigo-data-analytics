package omigo_core

object Utils {
  // len(s3://) = 5
  def splitS3Path(path: String): (String, String) = {
    val part1 = path.substring(5)
    val index = part1.indexOf("/")
    val bucketName = part1.substring(0, index)

    // boundary conditions
    val objectKey = if (index < part1.length() - 1) part1.substring(index + 1) else ""

    // remove trailing suffix
    val objectKey2 = if (objectKey.endsWith("/")) objectKey.substring(0, objectKey.length() - 1) else objectKey 

    // return
    return (bucketName, objectKey2)
  }
}

