package s3_file_handler 

import (
    "os"
    "log"
    "fmt"
    "errors"
    "strings"
    "bytes"
    "compress/gzip"
    "omigo/lokki/s3io_wrapper"
)

// create struct to hold local file
type ExtFile struct {
    Path string
    MinRecords int
    Records []string
    NumSuspiciousErrors int
    MaxAllowedErrors int
    S3Region string
    AWSProfile string
}

// method to instantiate class
func New(path string, minRecords int, maxErrors int) *ExtFile {
    return NewWithRegion(path, minRecords, maxErrors, "us-west-1", "default")
}

// create file reference with region
func NewWithRegion(path string, minRecords int, maxErrors int, s3Region string, awsProfile string) *ExtFile {
    arr := make([]string, 0)
    return &ExtFile { Path: path, MinRecords: minRecords, Records: arr, NumSuspiciousErrors: 0, MaxAllowedErrors: maxErrors, S3Region: s3Region, AWSProfile: awsProfile }
}

// persist only when there is valid data
func (c *ExtFile) Persist() error {
    // check if there is minimum amount of data or not
    if (len(c.Records) < c.MinRecords || c.HasReachedMaxErrors()) {
        return errors.New(fmt.Sprintf("The file doesnt have minimum amount of data or max errors reached. Found: %d, MinRecords: %d, NumSuspiciousErrors: %d, MaxAllowedErrors: %d", len(c.Records), c.MinRecords, c.NumSuspiciousErrors, c.MaxAllowedErrors))
    }

    // create a single string buffer
    allData := []byte(strings.Join(c.Records, "\n") + "\n")

    // check for gz extension
    if (strings.HasSuffix(c.Path, ".gz")) {
        // create a buffer to write gz content
        var buf bytes.Buffer
        zw := gzip.NewWriter(&buf)
        _, zwerr := zw.Write(allData)

        // check for error in compressing file content
        if (zwerr != nil) {
            log.Println("Error in writing gzip content: ", c.Path, zwerr)
            return zwerr
        }

        // close the writer
        zw.Close()

        // print how many bytes written
        // log.Println("compressing number of bytes:", numBytes, len(allData), buf.Len())

        // reassign original buffer
        allData = buf.Bytes()
    }

    // print message
    log.Printf("ExtFile persist called for %s, records: %d, errors: %d\n", c.Path, len(c.Records), c.NumSuspiciousErrors)

    // create error
    var err error = nil

    // use the correct persist method
    if (strings.HasPrefix(c.Path, "s3://")) {
        log.Println("Calling S3 persist file:", c.Path)
        err = c.persistS3File(allData)
    } else {
        log.Println("Calling local persist file:", c.Path)
        err = c.persistLocalFile(allData)
    }

    // check for error
    if (err != nil) {
        log.Println("Error in persisting file:", c.Path, c.NumSuspiciousErrors, err)
        return err
    }

    // return nil
    return nil
}

// get the directory and filename 
func (c *ExtFile) SplitLocalPath(path string) (string, string, error) {
    // check for validation
    if (strings.Index(path, "//") != -1) {
        return "", "", errors.New(fmt.Sprintf("This SplitLocalPath can not be used with multiple path separators: %s", path))
    }

    parts := strings.Split(path, "/")
    if (len(parts) > 1) {
        return strings.Join(parts[0:len(parts)-1], "/"), parts[len(parts)-1], nil
    } else {
        return "", path, nil
    }
}

// persist local file
func (c *ExtFile) persistLocalFile(buf []byte) error {
    // create directory
    directory, _, err := c.SplitLocalPath(c.Path)
    if (err != nil) {
        return err
    }

    if (directory != "") {
        errd := os.MkdirAll(directory, os.ModePerm)
        if (errd != nil) {
            return errd
        }
    }

    // create output file
    f, err := os.Create(c.Path)

    // check for error
    if (err != nil) {
        return err
    }

    // defer closing
    defer c.closeAndFreeze(f)

    // write
    _, err2 := f.Write(buf)
    return err2
}

// persist s3 file
func (c *ExtFile) persistS3File(buf []byte) error {
    return utils.WriteToS3(c.Path, c.S3Region, c.AWSProfile, buf)
}

// close and invalidate the buffer
func (c *ExtFile) closeAndFreeze(f *os.File) {
    f.Close()
    c.Records = nil
}

// write a single record into the buffer
func (c *ExtFile) WriteLine(line string) error {
    // check if the data buffer is valid
    if (c.Records == nil) {
        return errors.New("file is already closed.")
    }

    // check for max errors
    if (c.NumSuspiciousErrors > 0 && c.NumSuspiciousErrors >= c.MaxAllowedErrors) {
        return errors.New(fmt.Sprintf("NumErrors: %d, max allowed: %d", c.NumSuspiciousErrors, c.MaxAllowedErrors))
    }

    // append data
    c.Records = append(c.Records, line)

    return nil
}

// increment suspicious errors
func (c *ExtFile) IncrementSuspiciousErrors() {
    c.NumSuspiciousErrors++
}

// check if has reached the maximum errors
func (c *ExtFile) HasReachedMaxErrors() bool {
    return c.NumSuspiciousErrors > 0 && c.NumSuspiciousErrors >= c.MaxAllowedErrors
}

func main1() {
    localFile := NewWithRegion("s3://some-bucket/output-file.tsv.gz", 2, 1, "us-west-1", "test")

    localFile.WriteLine("This is a long line to write as content to a text file 1")
    localFile.WriteLine("This is a long line to write as content to a text file 2")
    localFile.WriteLine("This is a long line to write as content to a text file 3")

    err2 := localFile.Persist()
    if (err2 != nil) {
        log.Println("Error in calling persist:", err2)
    } else {
        log.Println("File written")
    }
}
