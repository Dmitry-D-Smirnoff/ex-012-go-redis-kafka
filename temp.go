package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"ex-012-go-redis-kafka/util"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type LogEntry struct {
	Operation  string             `bson:"operation" json:"operation"`
	AppEntity  string             `bson:"app_entity" json:"appEntity"`
	EntityName string             `bson:"entity_name" json:"entityName"`
	CreateDate primitive.DateTime `bson:"create_date" json:"createDate"`
}

const (
	kafkaConn = "ex-012-go-redis-kafka-dmitry-8002.aivencloud.com:25925"
	topic = "message-log"
	mongoBufferSize = 5
)

func initProducer()(sarama.SyncProducer, sarama.Consumer, error) {

	//TODO: if fails restore 3 paths to: C:\Users\d.smirnov\Downloads\_VPN_KEYS\Kafka\
	//serviceCertPath, err := filepath.Abs("./service.cert")
	//fmt.Println(serviceCertPath)
	keypair, err := tls.LoadX509KeyPair("./service.cert",
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
		Certificates: []tls.Certificate{keypair},
		RootCAs: caCertPool,
	}

	// init config, enable errors and notifications
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.Version = sarama.V0_10_2_0

	// init producer
	// AIVEN BROKER 1:
	//    SSL://35.228.129.49:25925,
	//    PLAINTEXT://ex-012-go-redis-kafka-1.aiven.local:25924,
	//    INTERNAL://[fda7:a938:5bfe:5fa6::a]:25932,
	//    SSL_PUBLIC://35.228.129.49:25935,
	//    SASL_SSL://35.228.129.49:25936
	// AIVEN BROKER 2:
	//    SSL://35.228.158.97:25925,
	//    PLAINTEXT://ex-012-go-redis-kafka-2.aiven.local:25924,
	//    INTERNAL://[fda7:a938:5bfe:5fa6::b]:25932,
	//    SSL_PUBLIC://35.228.158.97:25935,
	//    SASL_SSL://35.228.158.97:25936
	// AIVEN BROKER 3:
	//    SSL://35.228.15.106:25925,
	//    PLAINTEXT://ex-012-go-redis-kafka-3.aiven.local:25924,
	//    INTERNAL://[fda7:a938:5bfe:5fa6::c]:25932,
	//    SSL_PUBLIC://35.228.15.106:25935,
	//    SASL_SSL://35.228.15.106:25936
	brokers := []string{"35.228.129.49:25925","35.228.158.97:25925","35.228.15.106:25925"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	//TODO: For async:
	//producer, err := sarama.NewAsyncProducer(brokers, config)

	consumer, err := sarama.NewConsumer(brokers, config)

	return producer, consumer, err
}

func publish(logEntry LogEntry, producer sarama.SyncProducer) {

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
}

func consume(consumer sarama.Consumer) (chan *sarama.ConsumerMessage, []chan LogEntry) {

	partitionList, err := consumer.Partitions(topic) //get all partitions
	if err != nil {
		fmt.Println("Error consuming: ", err.Error())
	}

	fmt.Println("Partitions" + string(partitionList))

	messages := make(chan *sarama.ConsumerMessage, 256)
	channels := make([]chan LogEntry, len(partitionList))
	initialOffset := sarama.OffsetOldest //offset to start reading message from
	for i, partition := range partitionList {
		fmt.Println("Partition " + string(i))
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)
		go func(pc sarama.PartitionConsumer) {
			channels[i] = make(chan LogEntry)
			for message := range pc.Messages() {
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
				channels[i] <- content
				messages <- message //or call a function that writes to disk
			}
		}(pc)
	}

	return messages, channels
}

func merge(cs []chan LogEntry) <-chan LogEntry {
	var wg sync.WaitGroup
	out := make(chan LogEntry, mongoBufferSize)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan LogEntry) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func main() {

	redisKey1 := "time."+time.Now().Format("20060102.150405.000000000")
	redisValue1 := LogEntry{
		Operation:  "Update",
		AppEntity:  "Account",
		EntityName: "Jenny",
		CreateDate: primitive.NewDateTimeFromTime(time.Now()),
	}

	// create producer
	producer, consumer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	publish(redisValue1, producer)

	messages, channels := consume(consumer)
	for message := range messages{
		switch message.Topic {
		case topic:
			fmt.Println(string(message.Value))
			break
			// ...
		}

	}

	resultChannel := merge(channels)
	for logEntry := range resultChannel{
		fmt.Println(logEntry.EntityName)
	}


	redisClient := util.InitRedis()

	err = redisClient.SetKey(redisKey1, &redisValue1, time.Hour*3000)
	if err != nil {
		log.Fatalf("Error: %v", err.Error())
	}

	value2 := LogEntry{}
	err = redisClient.GetKey(redisKey1, &value2)
	if err != nil {
		log.Fatalf("Error: %v", err.Error())
	}

	fmt.Println("Update: " + value2.AppEntity)
	fmt.Println("Email: " + value2.EntityName)

	router := mux.NewRouter()
	router.HandleFunc("/api/log/recent/{limit}", util.GetLastLogEntries).Methods("GET")
	router.NotFoundHandler = http.HandlerFunc(util.HandleNotFound)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000" //localhost
	}
	err = http.ListenAndServe(":" + port, router)
	if err != nil {
		fmt.Print(err)
	}
/*

	if len(os.Args) > 2{
		if os.Args[1] == "populate"{
			numRecords,err := strconv.Atoi(os.Args[2])
			if err!=nil{
				fmt.Println( "Could not convert arguments provided, hence creating four entries")
				numRecords = 4
			}
			redis.Populate(numRecords)
			return
		}
	}

	router := mux.NewRouter()
	router.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pong !!\n"))
	})
	router.HandleFunc("/detail/{id}", VideoHandler)
	router.HandleFunc("/like/{id}", LikeHandler)
	router.HandleFunc("/popular/{num[0-9]+}", PopularHandler)
	http.Handle("/", router)
	banner.Print("video-feed")

	log.Println("Initializing redis pool: ")
	redis.Init()
	go kafka.InitProducer()
	go kafka.Consumer([]string{"likes", "upload"})

	port := os.Getenv("PORT")
	if port == "" {
		port = "4000" //localhost
	}
	err := http.ListenAndServe(":" + port, nil)
	if err != nil {
		log.Printf("Server error %v :", err)
	}
	log.Println("Video-Feed Listening on :4000")
   */
}
