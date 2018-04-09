package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cenkalti/backoff"
	"github.com/joho/godotenv"
)

// Part defines part information
type Part struct {
	number  int
	content []byte
}

const (
	maxRetries = 3
)

var awsAccessKeyID string
var awsSecretAccessKey string
var awsBucketRegion string
var awsBucketName string
var awsEndpoint string

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() / 2) // http://ascii.jp/elem/000/001/480/1480872/
}

func main() {
	err := godotenv.Load("aws.config")
	if err != nil {
		log.Fatal("Error loading .aws.config file")
	}

	awsAccessKeyID = os.Getenv("S3_ACCESS_KEY_ID")
	awsSecretAccessKey = os.Getenv("S3_SECRET_ACCESS_KEY")
	awsBucketRegion = os.Getenv("S3_BUCKET_REGION")
	awsBucketName = os.Getenv("S3_BUCKET_NAME")
	awsEndpoint = os.Getenv("S3_ENDPOINT")

	download(os.Args[1])
}

func download(path string, offset, size uint64) {
	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		log.Println("Bad credentials:", err)
	}
	cfg := aws.NewConfig().WithCredentials(creds).WithRegion(awsBucketRegion).WithEndpoint(awsEndpoint)
	svc := s3.New(session.New(), cfg)

	input := &s3.GetObjectInput{
		Bucket: aws.String(awsBucketName),
		Key:    aws.String(path),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)),
	}

	var output *s3.GetObjectOutput
	operation := func() error {
		var err error
		output, err = svc.GetObject(input)
		return err
	}

	notify := func(err error, duration time.Duration) {
		log.Printf("%v %v\n", duration, err)
	}

	err = backoff.RetryNotify(
		operation,
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetries),
		notify)
	if err != nil {
		log.Println(err)
		return
	}

	defer output.Body.Close()

	var buf bytes.Buffer

	buf.ReadFrom(output.Body)

	fmt.Println(path + " -> " + buf.String())
}
