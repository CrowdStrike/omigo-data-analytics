package omigo_core
import collection.JavaConverters._
import java.io.IOException
import java.nio.ByteBuffer
import java.util.Random
import software.amazon.awssdk.core.waiters.WaiterResponse
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.waiters.S3Waiter
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.HeadBucketResponse
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException

object S3Wrapper {
  val MaxDirectoryListKeys = 10000
  val s3 = S3Client.builder()
    .region(Region.of("us-west-1"))
    .build()

  def resolveRegionProfile(regionStr: String, profileStr: String): (String, String) = {
    // TODO: Implement this
    // return
    (regionStr, profileStr)
  }

  def putS3FileContent(bucketName: String, objectKey: String, barr: Array[Byte], regionStr: String, profileStr: String) {
    val objectRequest = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(objectKey)
      .build()

    s3.putObject(objectRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(barr)))
  }

  def putS3FileWithTextContent(bucketName: String, objectKey: String, text: String, regionStr: String, profileStr: String) {
    putS3FileContent(bucketName, objectKey, text.getBytes(), regionStr, profileStr)
  } 

  def checkPathExists(path: String, regionStr: String, profileStr: String): Boolean = {
    val (bucketName, objectKey) = Utils.splitS3Path(path)
    throw new RuntimeException("No need to implement yet")
  }

  def checkFileExists(path: String, regionStr: String, profileStr: String): Boolean = {
    val (regionStr2, profileStr2) = resolveRegionProfile(regionStr, profileStr)
    val (bucketName, objectKey) = Utils.splitS3Path(path)

    try {
      val objectRequest = HeadObjectRequest.builder()
        .bucket(bucketName)	
        .key(objectKey)
        .build()

      s3.headObject(objectRequest) != null
    } catch {
      case e: NoSuchKeyException => return false
      case e: Exception => throw e
    }
  }

  def getS3FileContent(bucketName: String, objectKey: String, regionStr: String, profileStr: String): Array[Byte] = {
    val (regionStr2, profileStr2) = resolveRegionProfile(regionStr, profileStr)

    try {
      val objectRequest = GetObjectRequest.builder()
        .bucket(bucketName)	
        .key(objectKey)
        .build()

      s3.getObjectAsBytes(objectRequest).asByteArray()
    } catch {
      case e: Exception => throw e
    }
  }

  def getS3FileContentAsText(bucketName: String, objectKey: String, regionStr: String, profileStr: String): String = {
    new String(getS3FileContent(bucketName, objectKey, regionStr, profileStr)) 
  }

  def getDirectoryListing(bucketName: String, objectKey: String, regionStr: String, profileStr: String): List[String] = {
    val (regionStr2, profileStr2) = resolveRegionProfile(regionStr, profileStr)

    try {
      var listObjectsReqManual = ListObjectsV2Request.builder()
        .bucket(bucketName)	
        .prefix(objectKey)
        .maxKeys(MaxDirectoryListKeys)
        .build()

      val buffer = new scala.collection.mutable.ListBuffer[String]()

      var done = false
      while (done == false) {
        var listObjResponse = s3.listObjectsV2(listObjectsReqManual)

        // add to buffer
        listObjResponse.contents().asScala.foreach({ content => buffer.append(content.key()) })

        // check if more entries are left
        if (listObjResponse.nextContinuationToken() == null)
          done = true

        // update the request object
        listObjectsReqManual = listObjectsReqManual.toBuilder()
          .continuationToken(listObjResponse.nextContinuationToken())
          .build()
      }

      // return
      buffer.toList
    } catch {
      case e: Exception => throw e
    }
  }

  def main(args: Array[String]): Unit = {
    val bucketName = "tsv-data-analytics-sample"
    val objectKey = "test-folder1/temp.txt"
    val content = args(0)
    S3Wrapper.putS3FileContent(bucketName, objectKey, content.getBytes(), null, null)
    println(S3Wrapper.getS3FileContentAsText(bucketName, objectKey, null, null)) 
    println(S3Wrapper.getDirectoryListing(bucketName, objectKey, null, null))
  }
}

