package event

import (

	"fmt"
	
	"log"
	"flag"
	"github.com/rs/xid"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)
var (
	stream = flag.String("stream", "amplitude-stream", "your stream name")
	region = flag.String("region", "us-east-1", "your AWS region")
)
var cnt int = 0

//Move the data to the Kinesis Stream.
func Kinesis(dat []byte) {
	// data := make(map[string]interface{})
	// err := json.Unmarshal(dat, &data)
	// if err != nil {
	// 	log.Println(err)
	// }

	// body2, _ := data["body"].(string)
	// body := []byte(body2)
	// data2 := make(map[string]interface{})
	// err2 := json.Unmarshal(body, &data2)
	// if err2 != nil {
	// 	log.Println(err)
	// }

	// var partitionKey = ""
	// if userId, ok := data2["user_id"].(string); ok && len(userId) > 0 {
	// 	partitionKey = userId
	// } else if userId, ok := data2["user_id"].(float64); ok && userId > 0 {
	// 	partitionKey = fmt.Sprintf("%f", userId)
	// } else if deviceId, ok := data2["device_id"].(string); ok && len(deviceId) > 0 {
	// 	partitionKey = deviceId
	// } else {
	// 	log.Println("unable to find partition key")
	// }
	guid := xid.New()

	

	flag.Parse()
	log.Printf("Starts a kinesis session.")
	s := session.New(&aws.Config{Region: aws.String(*region)})
	kc := kinesis.New(s)
	streamName := aws.String(*stream)

	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         dat,
		StreamName:   streamName,
		PartitionKey: aws.String(guid.String()),
	})
	if err != nil {
		fmt.Println("Error while inserting to kinesis stream")
		fmt.Println(err)
		fmt.Println("******************************")

		panic(err)
	} else {
		cnt = cnt + 1
		log.Println("inserted record!!",cnt)
		log.Println(putOutput)
		
	}

}	


