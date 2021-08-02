package data

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type LogEntry struct {
	Operation  string             `bson:"operation" json:"operation"`
	AppEntity  string             `bson:"app_entity" json:"appEntity"`
	EntityName string             `bson:"entity_name" json:"entityName"`
	CreateDate primitive.DateTime `bson:"create_date" json:"createDate"`
}

var topic string

func InitKafka()(sarama.SyncProducer, sarama.Consumer, error) {

	topic = os.Getenv("kafka_topic")
	if topic == ""{
		panic("No topic specified!")
	}

	//TODO: if fails restore 3 paths to: C:\Users\d.smirnov\Downloads\_VPN_KEYS\Kafka\
	//serviceCertPath, err := filepath.Abs("./service.cert")
	//fmt.Println(serviceCertPath)
	keyPair, err := tls.LoadX509KeyPair("service.cert",
		"service.key")
	if err != nil {
		log.Println(err)
		return nil, nil, err
	}

	caCert, err := ioutil.ReadFile("ca.pem")
	if err != nil {
		log.Println(err)
		return nil, nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		RootCAs: caCertPool,
	}

	// init config, enable errors and notifications
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.Version = sarama.V0_10_2_0

	//kafka URI = "ex-012-go-redis-data-dmitry-8002.aivencloud.com:25925"
	// init producer
	// AIVEN BROKER 1:
	//    SSL://35.228.129.49:25925,
	//    PLAINTEXT://ex-012-go-redis-data-1.aiven.local:25924,
	//    INTERNAL://[fda7:a938:5bfe:5fa6::a]:25932,
	//    SSL_PUBLIC://35.228.129.49:25935,
	//    SASL_SSL://35.228.129.49:25936
	// AIVEN BROKER 2:
	//    SSL://35.228.158.97:25925,
	//    PLAINTEXT://ex-012-go-redis-data-2.aiven.local:25924,
	//    INTERNAL://[fda7:a938:5bfe:5fa6::b]:25932,
	//    SSL_PUBLIC://35.228.158.97:25935,
	//    SASL_SSL://35.228.158.97:25936
	// AIVEN BROKER 3:
	//    SSL://35.228.15.106:25925,
	//    PLAINTEXT://ex-012-go-redis-data-3.aiven.local:25924,
	//    INTERNAL://[fda7:a938:5bfe:5fa6::c]:25932,
	//    SSL_PUBLIC://35.228.15.106:25935,
	//    SASL_SSL://35.228.15.106:25936
	//
	//brokers := []string{"35.228.129.49:25925","35.228.158.97:25925","35.228.15.106:25925"}
	brokerEnv := os.Getenv("kafka_brokers")
	if brokerEnv == ""{
		panic("No brokers specified!")
	}
	brokers := strings.Split(brokerEnv, ",")
	fmt.Printf("Current Kafka Connection: to %d Brokers: %s\n", len(brokers), brokerEnv)

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	//TODO: For async: IS IT NEEDED?
	//producer, err := sarama.NewAsyncProducer(brokers, config)

	consumer, err := sarama.NewConsumer(brokers, config)

	return producer, consumer, err
}

func Publish(logEntry LogEntry, producer sarama.SyncProducer) int64 {

	byteLogMessage, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}

	// publish sync
	msg := &sarama.ProducerMessage {
		Topic: topic,
		Value: sarama.ByteEncoder(byteLogMessage),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	// publish async
	//producer.Input() <- &sarama.ProducerMessage{

	fmt.Println("Partition: ", p)
	fmt.Printf("Offset: %d -> %d\n", o, o+1)
	return o+1
}

func Consume(consumer sarama.Consumer) chan *sarama.ConsumerMessage {

	partitionList, err := consumer.Partitions(topic) //get all partitions
	if err != nil {
		fmt.Println("Error consuming: ", err.Error())
	}

	messages := make(chan *sarama.ConsumerMessage, 256)
	initialOffset := sarama.OffsetNewest //offset to start reading message from
	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messages <- message //or call a function that writes to disk
			}
		}(pc)
	}

	return messages
}

func getTimeKey() string{
	return "time."+time.Now().Format("20060102.150405.000000000")
}

func ProcessMessages(redisCl *LogRedisClient, messageCh chan *sarama.ConsumerMessage, resultCh chan *LogEntry, id int){
	for message := range messageCh {
		switch message.Topic {
		case topic:
			var content LogEntry
			err := json.Unmarshal(message.Value, &content)
			if err != nil {
				content = LogEntry{
					Operation:  "BadMessage",
					AppEntity:  "NoEntity",
					EntityName: string(message.Value),
					CreateDate: primitive.NewDateTimeFromTime(time.Now()),
				}
			}
			currentKey := getTimeKey()
			err = redisCl.SetKey(currentKey, &content, time.Hour*3000)
			if err != nil {
				log.Fatalf("Error: %v", err.Error())
			}
			fmt.Printf("Processor #%d: Message saved to Redis with key = %s\n", id, currentKey)
			resultCh <- &content
			break
		default:
			fmt.Println("!!! Unknown Message. Not saved.")
		}
	}
}

func Merge(cs []chan *LogEntry) chan *LogEntry {
	var wg sync.WaitGroup
	out := make(chan *LogEntry, GetMongoBufferSize())

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c chan *LogEntry, id int) {
		fmt.Printf("Collector #%d: is ready\n", id)
		for n := range c {
			fmt.Printf("Collector #%d: value transferred\n", id)
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for i, c := range cs {
		go output(c, i)
	}
	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func FinishProcessing(resultCh chan *LogEntry){

	var logEntry *LogEntry
	size := GetMongoBufferSize()
	logEntries := make([]LogEntry, size)

	for{
		for i, _ := range logEntries {
			logEntry = <- resultCh
			logEntries[i] = *logEntry
		}
		fmt.Printf("Finisher: is saving %d entries to MongoDB -> ", size)
		InsertManyLogEntries(logEntries)
	}
}
