package main

import (
	"flag"
	"net/http"
	"time"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	kinesisproducer "github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/rs/xid"
	"os"
	"log"
)


var kinesis_producer *kinesisproducer.Producer
var AWS_ARN string
var qURL string
var  svc *sqs.SQS

var (
	ParamBatchCount         = flag.Int("batch-count", 100, "max number of items in a aggregrate record")
	ParamAggregateBatchSize = flag.Int("aggregate-batch-size", 4, "AggregateBatchSize determines max batch size to pass into the record")
	ParamFlushInterval      = flag.Int("flush-interval", 1, "Flush Interval")
	ParamStreamName         = flag.String("stream-name", "", "name of the kinesis stream")
	region					= flag.String("aws_region", "us-east-1", "Region where AWS Service is running")

)

func init() {

	flag.Parse()
	ACCOUNT_ID := os.Getenv("ACCOUNT_ID")
	ROLE_NAME := os.Getenv("ROLE_NAME")
	QUEUE_NAME := os.Getenv("QUEUE_NAME")
	
	log.Println("ACCOUNT_ID:", ACCOUNT_ID)
	log.Println("ROLE_NAME:", ROLE_NAME)
	log.Println("QUEUE_NAME:", QUEUE_NAME)

	AWS_ARN = "arn:aws:iam::" + ACCOUNT_ID + ":role/" + ROLE_NAME
	qURL = "https://sqs."+ aws.StringValue(region) + ".amazonaws.com/" + ACCOUNT_ID + "/" + QUEUE_NAME
	

	/*---------------Create a kinesis and SQS client----------------------------------*/
	
	IntervalInDuration := time.Duration(*ParamFlushInterval) * time.Second
	sess := session.Must(session.NewSession(&aws.Config{Region: region}))
	creds := stscreds.NewCredentials(sess, AWS_ARN)
	client := kinesis.New(sess, &aws.Config{Credentials: creds})
	svc = sqs.New(sess, &aws.Config{Credentials: creds})




	/*-------------Intilializes kpl producer stream--------------------------*/
	//Batch count: max number of items in a aggregrate record.
	// AggregateBatchSize determines max batch size to pass into the record.
	// FlushInterval is a regular interval for flushing the buffer.

	kinesis_producer = kinesisproducer.New(&kinesisproducer.Config{
		StreamName:         *ParamStreamName,
		BatchCount:         *ParamBatchCount,
		AggregateBatchSize: *ParamAggregateBatchSize,
		FlushInterval:      IntervalInDuration,
		Client:             client,
	})
	kinesis_producer.Start()
}



func kinesisHandler(w http.ResponseWriter, r *http.Request, jsonData []byte) {
	
	// generate a random number to be used as partition key 
	guid := xid.New()
	// insert data to the kinesis stream
	err := kinesis_producer.Put([]byte(string(jsonData)+"\n"),
	guid.String())
	// if kinesis insertion failed, insert the data to SQS FIFO queue.
	if err != nil {

		writeError(w, r, "unable_to_write in Kinesis Stream", 500)
		result, er := svc.SendMessage(&sqs.SendMessageInput{
			MessageGroupId: aws.String("1"),
			MessageDeduplicationId: aws.String(guid.String()),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
            "Error": &sqs.MessageAttributeValue{
                DataType:    aws.String("String"),
                StringValue: aws.String(err.Error()),
            },
            
        },
			MessageBody: aws.String(string(jsonData)),
			QueueUrl:    &qURL,
		})

		if er != nil {
			log.Println("Error", er)
			return
		}

		log.Println("Successfully inserted to SQS", *result.MessageId)
	}

	
}


func stopKinesisProducer() {
	kinesis_producer.Stop()
}

