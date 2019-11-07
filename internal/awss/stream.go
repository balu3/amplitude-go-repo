package awss

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"log"
	
)

var kc *kinesis.Kinesis

func init() {
	region := "us-east-1"
	s := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	kc = kinesis.New(s)
	log.Printf("loading kinesis \n")
}

func KinesisPutRecords(streamName string, records []*kinesis.PutRecordsRequestEntry) error {
	chunkSize := 500
	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}
		_, err := kc.PutRecords(&kinesis.PutRecordsInput{
			StreamName: &streamName,
			Records:    records[i:end],
		})
		if err != nil {
			
			log.Println(err)
			return err
		} else {
			log.Printf("@@@@@@@@@@@@@ records inserted @@@@@@@@@@@@@@@@@@@@@@@@@")
		}
	}
	return nil
}
