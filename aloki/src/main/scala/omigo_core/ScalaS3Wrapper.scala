package omigo_core
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

object ScalaS3Wrapper {
    def putS3FileContent(bucketName: String, objectKey: String, barr: Array[Byte], regionStr: String, profileStr: String) {
        val region = if (regionStr != null) Region.of(regionStr) else Region.US_WEST_2

        val s3 = S3Client.builder()
            .region(region)
            .build()

        val objectRequest = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(objectKey)
            .build()

        s3.putObject(objectRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(barr)))
    }

    def main(args: Array[String]): Unit = {
        val bucketName = "tsv-data-analytics-sample"
        val objectKey = "test-folder1/temp.txt"
        val content = args(0)
        ScalaS3Wrapper.putS3FileContent(bucketName, objectKey, content.getBytes(), "us-west-1", "default")
    }
}

