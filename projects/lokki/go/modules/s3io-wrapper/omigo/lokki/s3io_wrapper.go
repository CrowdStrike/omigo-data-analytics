package s3io_wrapper 

import (
    "bytes"
    "compress/gzip"
    "log"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "io/ioutil"
    "math"
    "math/rand"
    "net/http"
    "os"
    "fmt"
    "regexp"
    "strings"
)

type S3FileMeta struct {
    Path string
    Size int64
    IsDirectory bool
}

func MinInt(a int, b int) int {
    return int(math.Min(float64(a), float64(b)))
}

func GetHeaderMap(line string) map[string]int {
    // create map
    mp := make(map[string]int)

    // populate map
    fields := strings.Split(line, "\t")
    for i := 0; i < len(fields); i++ {
        mp[fields[i]] = i
    }

    return mp
}

func GetLocalFileContent(path string) ([]byte, error) {
    // open file
    f, err1 := os.Open(path)

    // check for error
    if (err1 != nil) {
        log.Println("GetLocalFileContent : error in opening file:", path, err1)
        return nil, err1
    }

    // defer closing
    defer f.Close()

    // read data
    barr, err2 := ioutil.ReadAll(f)
    if (err2 != nil) {
        log.Println("GetLocalFileContent : error in reading file:", path, err2)
        return nil, err2
    }

    // check if this was compressed file or not
    if (strings.HasSuffix(path, ".gz")) {
        barr2, err3 := DecompressBuffer(barr)

        // check for error
        if (err3 != nil) {
            log.Println("GetLocalFileContent: Error in reading gzip contents: ", path, err3)
            return nil, err3
        }

        // reassignment of buffer
        barr = barr2
    }

    return barr, nil
}

func GetLocalFileLinesCount(path string) (int, error) {
    buf, err := GetLocalFileContent(path)

    // check for error
    if (err != nil) {
        log.Println("Error in reading contents: ", path, err)
        return -1, err
    }

    // get the string content
    fileContent := strings.TrimSuffix(string(buf), "\n")
    lines := strings.Split(fileContent, "\n")
    numLines := len(lines)

    return numLines, nil
}

func CreateS3Session(s3Region string, awsProfile string) (*session.Session, error) {
    // create session
    sess, err := session.NewSessionWithOptions(session.Options{
        // pick the correct profile
        Profile: awsProfile,

        // region
        Config: aws.Config{ Region: aws.String(s3Region) },

        // not sure of this config
        SharedConfigState: session.SharedConfigEnable,
    })

    // check for error in creating session
    if (err != nil) {
        log.Println("Error in opening s3 session:", s3Region, awsProfile, err)
        return nil, err
    }

    // return
    return sess, nil
}

func ListS3Objects(path string, s3Region string, awsProfile string) (*s3.ListObjectsV2Output, error) {
    // create session
    sess, err := CreateS3Session(s3Region, awsProfile)

    // check for error
    if (err != nil) {
        log.Println("Error in ListS3Objects: ", path, err)
        return nil, err
    }

    // svc
    svc := s3.New(sess)

    // get bucket
    bucket, objectKey := SplitS3Path(path)

    // objectKey must end with the delimiter
    if (strings.HasSuffix(objectKey, "/") == false) {
        objectKey = objectKey + "/"
    }

    // list objects
    resp, err2 := svc.ListObjectsV2(&s3.ListObjectsV2Input{
        Bucket: aws.String(bucket),
        Prefix: aws.String(objectKey),
        Delimiter: aws.String("/"),
        MaxKeys: aws.Int64(1000000),
    })

    // check for error
    if (err2 != nil) {
        log.Println("Error in ListS3Objects: ", path, err2)
        return nil, err
    }

    // return response
    return resp, nil
}

func ListS3ObjectsMeta(path string, s3Region string, awsProfile string) ([]S3FileMeta, error) {
    // get the bucket and key
    bucket, _ := SplitS3Path(path)

    // get the objects
    resp, err := ListS3Objects(path, s3Region, awsProfile)

    // check for error
    if (err != nil) {
        log.Println("Error in ListS3Objects: ", path, err)
        return nil, err
    }

    // create response array
    result := make([]S3FileMeta, 0, 100)

    // get the files and directories separately

    // iterate over all responses in Contents for files
    for _, item := range resp.Contents {
        if (strings.HasSuffix(*item.Key, "/") == false) {
            result = append(result, S3FileMeta { Path: "s3://" + bucket + "/" + *item.Key, Size: *item.Size, IsDirectory: false })
        }
    }

    // iterate over CommonPrefixes for directories
    for _, item := range resp.CommonPrefixes {
        result = append(result, S3FileMeta { Path: "s3://" + bucket + "/" + *item.Prefix, Size: 0, IsDirectory: true })
    }

    // return response
    return result, nil
}

// TODO: make it better
func SplitS3Path(path string) (string, string) {
    // parse the s3 bucket and the object path
    parts := strings.SplitAfterN(path, "/", 4)

    // [s3:/ / <bucket-name>/ <path>/]
    if (len(parts) ==  4) {
        // take the bucket
        bucket := parts[2]
        if (strings.HasSuffix(bucket, "/")) {
            bucket = bucket[0: len(bucket)-1]
        }
        objectKey := parts[3]
        return bucket, objectKey
    } else {
        bucket := parts[2]
        if (strings.HasSuffix(bucket, "/")) {
            bucket = bucket[0: len(bucket)-1]
        }
        return bucket, ""
    }
}

func GetS3FileLinesCount(path string, s3Region string, awsProfile string) (int, error) {
    buf, err := GetS3FileContent(path, s3Region, awsProfile)

    // check for error
    if (err != nil) {
        log.Println("Error in reading contents: ", path, err)
        return -1, err
    }

    // get the string content
    fileContent := strings.TrimSuffix(string(buf), "\n")
    lines := strings.Split(fileContent, "\n")
    numLines := len(lines)

    return numLines, nil
}

func ParseImageFileBaseName(name string) string {
    // split by path separator
    parts := regexp.MustCompile(`[\\/]`).Split(name, -1)

    // take the last part
    if (len(parts) > 0) {
        return parts[len(parts) - 1]
    } else {
        return name
    }
}

func DecompressBuffer(barr []byte) ([]byte, error) {
    buf := bytes.NewBuffer(barr)
    zr, err1 := gzip.NewReader(buf)

    // check for error in initializing gzip reader
    if (err1 != nil) {
        return nil, err1
    }

    // read all content
    barr2, err2 := ioutil.ReadAll(zr)
    zr.Close()

    return barr2, err2
}

func Chop(str string) string {
    return strings.TrimSuffix(str, "\n")
}

func GetFloat64Value(mp map[string]interface{}, key string) float64 {
    value, ok := mp[key]
    if ok == true {
        return value.(float64)
    } else {
        return 0
    }
}

func GetStringValue(mp map[string]interface{}, key string) string {
    value, ok := mp[key]
    if ok == true {
        return value.(string)
    } else {
        return ""
    }
}

func GetStringArrayValue(mp map[string]interface{}, key string) []string {
    value, ok := mp[key]
    if ok == true {
        arr := value.([]interface{})
        result := make([]string, 0, 10)
        for _, v := range arr {
            result = append(result, v.(string))
        }
        return result
    } else {
        return []string{}
    }
}

func LocalFileExists(path string) bool {
    if _, err := os.Stat(path); os.IsNotExist(err) {
        return false
    } else {
        return true
    }
}

// TODO: Return error properly
func S3PathExists(path string, s3Region string, awsProfile string) bool {
    sess, err := CreateS3Session(s3Region, awsProfile)

    // check for error in creating session
    if (err != nil) {
        log.Println("Error in opening s3 session:", path, err)
        return false
    }

    // get bucket
    bucket, objectKey := SplitS3Path(path)

    // do a head operation in s3
    svc := s3.New(sess)
    _, err2 := svc.HeadObject(&s3.HeadObjectInput{
        Bucket: aws.String(bucket),
        Key: aws.String(objectKey),
    })

    // check for error
    if (err2 != nil) {
        return false
    } else {
        return true
    }
}

func S3DirExists(path string, s3Region string, awsProfile string) bool {
    if (strings.HasSuffix(path, "/") == false) {
        path = path + "/"
    }

    return S3PathExists(path, s3Region, awsProfile)
}

func S3FileExists(path string, s3Region string, awsProfile string) bool {
    return S3PathExists(path, s3Region, awsProfile)
}

// TODO: Return error properly
func FileExists(path string, s3Region string, awsProfile string) bool {
    if (strings.HasPrefix(path, "s3://")) {
        return S3FileExists(path, s3Region, awsProfile)
    } else {
        return LocalFileExists(path)
    }
}

func GetS3FileContent(path string, s3Region string, awsProfile string) ([]byte, error) {
    // create session
    sess, err := CreateS3Session(s3Region, awsProfile)

    // check for error in creating session
    if (err != nil) {
        log.Println("Error in opening s3 session:", path, err)
        return nil, err
    }

    // parse the s3 bucket and the object path
    bucket, objectKey := SplitS3Path(path)

    // create buffer to write
    writeAtBuf := aws.NewWriteAtBuffer([]byte{})

    // create downloader object
    downloader := s3manager.NewDownloader(sess)

    // download
    _, err2 := downloader.Download(writeAtBuf, &s3.GetObjectInput{
        Bucket: aws.String(bucket),
        Key:    aws.String(objectKey),
    })

    // check for error
    if (err2 != nil) {
        log.Println("Error in getting object:", path, err2)
        return nil, err2
    }

    // get bytes array
    buf := writeAtBuf.Bytes()

    // check for compression
    if (strings.HasSuffix(path, ".gz")) {
        buf2, err3 := DecompressBuffer(buf)
        if (err3 != nil) {
            log.Println("Error in getting object:", path, err3)
            return nil, err3
        }

        // reassignment of buffer
        buf = buf2
    }

    return buf, nil
}

func WriteToS3(path string, s3Region string, awsProfile string, buf []byte) error {
    // create session
    sess, err := CreateS3Session(s3Region, awsProfile)

    // check for error in creating session
    if (err != nil) {
        log.Println("Error in opening s3 session:", path, err)
        return err
    }

    // parse the s3 bucket and the object path
    bucket, objectKey := SplitS3Path(path)

    // put object
    svc := s3.New(sess)
    _, err2 := svc.PutObject(&s3.PutObjectInput{
        Bucket:               aws.String(bucket),
        Key:                  aws.String(objectKey),
        ACL:                  aws.String("private"),
        Body:                 bytes.NewReader(buf),
        ContentLength:        aws.Int64(int64(len(buf))),
        ContentType:          aws.String(http.DetectContentType(buf)),
        ContentDisposition:   aws.String("attachment"),
        ServerSideEncryption: aws.String("AES256"),
    })

    // return error
    return err2
}

func GetLinesCount(path string, s3Region string, awsProfile string) (int, error) {
    if (strings.HasPrefix(path, "s3://")) {
        return GetS3FileLinesCount(path, s3Region, awsProfile)
    } else {
        return GetLocalFileLinesCount(path)
    }
}

func GetFileContent(path string, s3Region string, awsProfile string) ([]byte, error) {
    if (strings.HasPrefix(path, "s3://")) {
        return GetS3FileContent(path, s3Region, awsProfile)
    } else {
        return GetLocalFileContent(path)
    }
}

func GetFileContentAsText(path string, s3Region string, awsProfile string) (string, error) {
    buf, err := GetFileContent(path, s3Region, awsProfile)

    if (err != nil) {
        log.Println("Error in getting file content:", path, err)
        return "", err
    }

    fileContent := strings.TrimSuffix(string(buf), "\n")
    return fileContent, nil
}

func CreateS3DownloaderWithPartSize(s3Region string, awsProfile string, partSize int64) (*s3manager.Downloader, error) {
    sess, err := CreateS3Session(s3Region, awsProfile)

    // check for error
    if (err != nil) {
        log.Println("Error in CreateS3Session:", err)
        return nil, err
    }

    svc := s3.New(sess)

    // create downloader
    downloader := s3manager.NewDownloaderWithClient(svc, func (d *s3manager.Downloader) {
        d.PartSize = partSize
    })

    // return
    return downloader, nil
}

func CreateS3DownloaderWithPool(s3Region string, awsProfile string, poolSize int) (*s3manager.Downloader, error) {
    sess, err := CreateS3Session(s3Region, awsProfile)

    // check for error
    if (err != nil) {
        log.Println("Error in CreateS3Session:", err)
        return nil, err
    }

    svc := s3.New(sess)

    // create downloader
    downloader := s3manager.NewDownloaderWithClient(svc, func (d *s3manager.Downloader) {
        d.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(poolSize)
    })

    // return
    return downloader, nil
}

func CreateDir(path string, s3Region string, awsProfile string) error {
    if (strings.HasPrefix(path, "s3://")) {
        emptyData := []byte("")
        if (strings.HasSuffix(path, "/") == false) {
            path = path + "/"
        }
        return WriteToS3(path, s3Region, awsProfile, emptyData)
    } else {
        return os.MkdirAll(path, 0700)
    }
}

const UUIDSampler = "0123456789abcdef"
func GetUUIDSamplePrefix(randGen *rand.Rand, n int) string {
    result := ""
    for i := 0; i < n; i++ {
        letter := string(UUIDSampler[randGen.Intn(16)])
        result = result + letter
    }
    return result
}


// utility method to read from map and get "" string when value is nil
func NilIntAsEmptyString(mp map[string]interface{}, key string) string {
    if val, ok := mp[key]; ok {
        if (val != nil) {
            return fmt.Sprintf("%d", int64(val.(float64)))
        } else {
            return ""
        }
    } else {
        return ""
    }
}

// utility method to read from map and get "" string when value is nil
func NilAsEmptyString(mp map[string]interface{}, key string) string {
    if val, ok := mp[key]; ok {
        if (val != nil) {
            return val.(string)
        } else {
            return ""
        }
    } else {
        return ""
    }
}

