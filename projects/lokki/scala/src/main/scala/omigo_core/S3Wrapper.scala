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

  def resolve_region_profile(s3_region: String, aws_profile: String): (String, String) = {
    // TODO: Implement this
    // return
    (s3_region, aws_profile)
  }

  def create_session_key(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_session(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_session_cache(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_resource(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_resource_cache(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_client(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_client_cache(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_bucket(bucket_name: String, s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_bucket_cache(bucket_name: String, s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_file_content(bucket_name: String, object_key: String, s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_s3_file_content(bucket_name: String, object_key: String, s3_region: String = null, aws_profile: String = null): Array[Byte] = {
    val (s3_region2, aws_profile2) = resolve_region_profile(s3_region, aws_profile)

    try {
      val objectRequest = GetObjectRequest.builder()
        .bucket(bucket_name)	
        .key(object_key)
        .build()

      s3.getObjectAsBytes(objectRequest).asByteArray()
    } catch {
      case e: Exception => throw e
    }
  }

  def get_s3_file_content_as_text(bucket_name: String, object_key: String, s3_region: String = null, aws_profile: String = null): String = {
    var barr = get_s3_file_content(bucket_name, object_key, s3_region = s3_region, aws_profile = aws_profile)
    if (object_key.endsWith(".gz")) {
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
    } else if (object_key.endsWith(".zip")) {
      throw new Exception("zip file extraction is not supported")
    }

    new String(barr)
  }

  def get_file_content_as_text(bucket_name: String, object_key: String, s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def check_path_exists(path: String, s3_region: String = null, aws_profile: String = null): Boolean = {
    val (bucket_name, object_key) = Utils.split_s3_path(path)
    throw new RuntimeException("No need to implement yet")
  }

  def check_file_exists(path: String, s3_region: String = null, aws_profile: String = null): Boolean = {
    val (s3_region2, aws_profile2) = resolve_region_profile(s3_region, aws_profile)
    val (bucket_name, object_key) = Utils.split_s3_path(path)

    try {
      val objectRequest = HeadObjectRequest.builder()
        .bucket(bucket_name)	
        .key(object_key)
        .build()

      s3.headObject(objectRequest) != null
    } catch {
      case e: NoSuchKeyException => return false
      case e: Exception => throw e
    }
  }

  def put_file_content(bucket_name: String, object_key: String, barr: Array[Byte], s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def put_s3_file_content(bucket_name: String, object_key: String, barr: Array[Byte], s3_region: String = null, aws_profile: String = null) {
    val objectRequest = PutObjectRequest.builder()
      .bucket(bucket_name)
      .key(object_key)
      .build()

    s3.putObject(objectRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(barr)))
  }

  def put_file_with_text_content(bucket_name: String, object_key: String, text: String, s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def put_s3_file_with_text_content(bucket_name: String, object_key: String, text: String, s3_region: String = null, aws_profile: String = null) {
    var barr = text.getBytes()
    if (object_key.endsWith(".gz")) {
        val byteStream = new ByteArrayOutputStream(barr.length)
        val zipStream = new GZIPOutputStream(byteStream)
        zipStream.write(barr)
        zipStream.close()
        barr = byteStream.toByteArray()
    } else if (object_key.endsWith(".zip")) {
        val byteStream = new ByteArrayOutputStream(barr.length)
        val zipStream = new ZipOutputStream(byteStream)
        zipStream.write(barr)
        zipStream.close()
        barr = byteStream.toByteArray()
    }
    put_s3_file_content(bucket_name, object_key, barr, s3_region = s3_region, aws_profile = aws_profile)
  } 

  def resolve_s3_region_aws_profile(s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  // def __get_all_s3_objects__(s3, **base_kwargs) = {
  //   throw new RuntimeException("Not implemented for Java")
  // }

  // TODO: The actual implementation is complex and inefficient. python needs fixing too
  def get_directory_listing(path: String, filter_func: (String) => Boolean = null, ignore_if_missing: Boolean = false, skip_exist_check: Boolean = false,
    s3_region: String = null, aws_profile: String = null): List[String] = {
    val (s3_region2, aws_profile2) = resolve_region_profile(s3_region, aws_profile)
    val (bucket_name, object_key) = Utils.split_s3_path(path)

    // validation
    if (check_path_exists(path, s3_region = s3_region, aws_profile = aws_profile) == false) {
        if (ignore_if_missing == false) {
          throw new RuntimeException("Directory does not exist: " + path)
        } else {
            Utils.debug("Directory does not exist: %s".format(path))
            List.empty
        } 
    }

    try {
      var listObjectsReqManual = ListObjectsV2Request.builder()
        .bucket(bucket_name)	
        .prefix(object_key)
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
      val filenames = buffer
        .map({ t => t.substring(object_key.length + 1) })
        .map({ t => t.split("/")(0) })
        .filter({ t => t != "" })
        .map({ t => path + "/" + t })
        .distinct
        .toList

      // exist check
      val filenames2 = {
        if (skip_exist_check == false)
          filenames.filter({ t => check_path_exists(t, s3_region = s3_region, aws_profile = aws_profile) })
        else
          filenames
      }

      // apply filter
      val filenames3 = {
        if (filter_func != null)
          filenames2.filter({ p => filter_func(p) })
        else
          filenames2
      }

      filenames3 
    } catch {
      case e: Exception => throw e
    }
  }

  def delete_file(path: String, ignore_if_missing: Boolean = false, s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }

  def get_last_modified_time(path: String, ignore_if_missing: Boolean = false, s3_region: String = null, aws_profile: String = null) = {
    throw new RuntimeException("Not implemented for Java")
  }


  // def main(args: Array[String]): Unit = {
  //   val bucket_name = "tsv-data-analytics-sample"
  //   val object_key = "test-folder1/temp.txt.zip"
  //   val content = args(0)
  //   // S3Wrapper.put_s3_file_with_text_content(bucket_name, object_key, content, null, null)
  //   // println(S3Wrapper.get_s3_file_content_as_text(bucket_name, object_key, null, null)) 
  //   println(S3Wrapper.get_directory_listing(args(0), null, false, null, null))
  // }
}

