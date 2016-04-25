package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/config"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/github.com/aws/aws-sdk-go/aws"
)

// SQSBroker represents a SQS broker
type SQSBroker struct {
	config              *config.Config
	queueUrl			string
	accessKey			string
	secretKey			string
	region				string
	registeredTaskNames []string
	retry               bool
	retryFunc           func()
	stop            	bool
}

// NewSQSBroker creates new SQSBroker instance
func NewSQSBroker(cnf *config.Config, queueUrl, accessKey, secretKey, region string) Broker {
	return Broker(&SQSBroker{
		config: cnf,
		retry:  true,
		stop: false,
		queueUrl: queueUrl,
		accessKey: accessKey,
		secretKey: secretKey,
		region: region,
	})
}

// SetRegisteredTaskNames sets registered task names
func (sqsBroker *SQSBroker) SetRegisteredTaskNames(names []string) {
	sqsBroker.registeredTaskNames = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (sqsBroker *SQSBroker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range sqsBroker.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// StartConsuming enters a loop and waits for incoming messages
func (sqsBroker *SQSBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	if sqsBroker.retryFunc == nil {
		sqsBroker.retryFunc = utils.RetryClosure()
	}
	
	svc, err := sqsBroker.open()
	// Maybe we need to know how to close it
	if err != nil {
		sqsBroker.retryFunc()
		return sqsBroker.retry, err
	}
	
	sqsBroker.retryFunc = utils.RetryClosure()
	
	sqsBroker.stopChan = make(chan int)
	
	log.Printf("[*] Waiting for messages. To exist press CTRL+C")
	
	if err := sqsBroker.consume(svc, taskProcessor); err != nil {
		return sqsBroker.retry, err
	}
	
	return sqsBroker.retry, nil
}

// StopConsuming quits the consumer
func (sqsBroker *SQSBroker) StopConsuming() {
	sqsBroker.stop = true
	sqsBroker.retry = false
}

// Publish places a new message on the default queue
func (sqsBroker *SQSBroker) Publish(signature *signatures.TaskSignature) error {
	svc, err := sqsBroker.open()
	// Maybe we need to know how to close it
	if err != nil {
		return err
	}
	
	message, err := json.Marshal(signature)
	if err != nil {
		return false, fmt.Errorf("JSON Encode Message: %v", err)
	}
	
	params := &sqs.SendMessageInput{
		MessageBody: aws.String(message), // Required
		QueueUrl: aws.String(sqsBroker.queueUrl), // Required
		DelaySeconds: aws.Int64(1),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{},
	}
	resp, err := svc.SendMessage(params)
	if err != nil {
		return false, fmt.Errorf("SQS Send Message: %v", err)
	}
	
	return true, nil
	
}

// Consume a single message
func (sqsBroker *SQSBroker) consumeOne(message *sqs.ReceiveMessageOutput, taskProcessor TaskProcessor) {
	if message.Body == nil {
		log.Printf("Received empty message")
		return
	}		
	
	log.Printf("Received new message: %s", *message.Body)
	
	signature := signatures.TaskSignature{}
	if err := json.Unmarshal(d.Body, &signature); err != nil {
		log.Printf("Message is not a valid signature")
	}
	
	if !sqsBroker.IsTaskRegistered(signature.Name) {
		log.Printf("Task was not registered")
		return
	}
	
	// Process
	if err := taskProcessor.Process(&signature); err != nil {
		log.Printf("Error while processing task: %v", err)
	}
	
	// Delete	
	params := &sqs.DeleteMessageInput{
		QueueUrl: aws.String(sqsBroker.queueUrl),
		ReceiptHandle: aws.String(message.ReceiptHandle),
	}
	resp, err := svc.DeleteMessage(params)

	if err != nil {
		log.Printf("SQS Delete Message: %v", err)
		return
	}
	
}

// Consume messages...
func (sqsBroker *SQSBroker) consumeHelper(svc *sqs.SQS, taskProcessor TaskProcessor) error {
	params := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(sqsBroker.queueUrl),
		AttributeNames: []*string{},
		MaxNumberOfMessages: aws.Int64(2),
		MessageAttributeNames: []*string{},
		VisibilityTimeout: aws.Int64(900), // MaxVisibility timeout
		WaitTimeSeconds: aws.Int64(1),
	}
	resp, err := svc.ReceiveMessage(params)
	if err != nil {
		return fmt.Errorf("SQS Receive Message: %v", err)
	}
	for _, message := range resp.Messages {
		go func() {
			sqsBroker.consumeOne(message, taskProcessor)
		}
	}
}

// Consumes messages...
func (sqsBroker *SQSBroker) consume(svc *sqs.SQS, taskProcessor TaskProcessor) error {
	for {
    	<-time.After(1 * time.Second)
		if sqsBroker.stop {
			break
		}
    	go sqsBroker.consumeHelper(svc, taskProcessor)
	}
	return nil
}

// Connects to the message queue, opens a channel, declares a queue
func (sqsBroker *SQSBroker) open() (*sqs.SQS, error) {
	var (
		svc       *sqs.SQS
	)

	// Connect
	creds := credentials.NewStaticCredentials(sqsBroker.accessKey, sqsBroker.secretKey, "")
	config := aws.config.NewConfig().WithCredentials(creds).WithRegion(sqsBroker.region)
	svc = sqs.New(session.New(), config)

	return svc, nil
}
