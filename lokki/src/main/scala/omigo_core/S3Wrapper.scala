package omigo_core
import collection.JavaConverters._
// import scala.jdk.CollectionConverters
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.Random
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
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

  def put_s3_file_content(bucketName: String, objectKey: String, barr: Array[Byte], regionStr: String, profileStr: String) {
    val objectRequest = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(objectKey)
      .build()

    s3.putObject(objectRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(barr)))
  }

  def put_s3_file_with_text_content(bucketName: String, objectKey: String, text: String, regionStr: String, profileStr: String) {
    var barr = text.getBytes()
    if (objectKey.endsWith(".gz")) {
        val byteStream = new ByteArrayOutputStream(barr.length)
        val zipStream = new GZIPOutputStream(byteStream)
        zipStream.write(barr)
        zipStream.close()
        barr = byteStream.toByteArray()
    } else if (objectKey.endsWith(".zip")) {
        val byteStream = new ByteArrayOutputStream(barr.length)
        val zipStream = new ZipOutputStream(byteStream)
        zipStream.write(barr)
        zipStream.close()
        barr = byteStream.toByteArray()
    }
    put_s3_file_content(bucketName, objectKey, barr, regionStr, profileStr)
  } 

  def check_path_exists(path: String, regionStr: String, profileStr: String): Boolean = {
    val (bucketName, objectKey) = Utils.splitS3Path(path)
    throw new RuntimeException("No need to implement yet")
  }

  def check_file_exists(path: String, regionStr: String, profileStr: String): Boolean = {
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

  def get_s3_file_content(bucketName: String, objectKey: String, regionStr: String, profileStr: String): Array[Byte] = {
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

  def get_s3_file_content_as_text(bucketName: String, objectKey: String, regionStr: String, profileStr: String): String = {
    var barr = get_s3_file_content(bucketName, objectKey, regionStr, profileStr)
    if (objectKey.endsWith(".gz")) {
      val byteStream = new ByteArrayInputStream(barr)
      val inputStream = new GZIPInputStream(byteStream)
      val baos = new ByteArrayOutputStream()
      var len = 0
      var buffer = Array.fill[Byte](1024)(0) 
      len = inputStream.read(buffer)
      while (len > 0) {
          baos.write(buffer, 0, len)
          len = inputStream.read(buffer)
      }
      barr = baos.toByteArray()
    } else if (objectKey.endsWith(".zip")) {
      throw new Exception("zip file extraction is not supported")
    }

    new String(barr)
  }

  // TODO: The actual implementation is complex and inefficient. python needs fixing too
  def get_directory_listing(path: String, filterFunc: String, failIfMissing: Boolean, regionStr: String, profileStr: String): List[String] = {
    val (regionStr2, profileStr2) = resolveRegionProfile(regionStr, profileStr)
    val (bucketName, objectKey) = Utils.splitS3Path(path)

    // TODO: filterFunc is a function

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
      buffer
        .map({ t => t.substring(objectKey.length + 1) })
        .map({ t => t.split("/")(0) })
        .filter({ t => t != "" })
        .map({ t => path + "/" + t })
        .distinct
        .toList
    } catch {
      case e: Exception => throw e
    }
  }

  def delete_file(path: String, fail_if_missing: Boolean, region: String, profile: String): Unit = {
    throw new Exception("Not implemented in Java")
  }

  def get_last_modified_time(path: String, fail_if_missing: Boolean, region: String, profile: String): Unit = {
    throw new Exception("Not implemented in Java")
  }

  def main(args: Array[String]): Unit = {
    val bucketName = "tsv-data-analytics-sample"
    val objectKey = "test-folder1/temp.txt.zip"
    val content = args(0)
    // S3Wrapper.put_s3_file_with_text_content(bucketName, objectKey, content, null, null)
    // println(S3Wrapper.get_s3_file_content_as_text(bucketName, objectKey, null, null)) 
    println(S3Wrapper.get_directory_listing(args(0), null, false, null, null))
  }
}

