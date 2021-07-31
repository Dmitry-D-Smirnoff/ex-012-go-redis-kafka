package main

import (
	"crypto/tls"
	"crypto/x509"
	"ex-012-go-redis-kafka/util"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io/ioutil"
	"log"
	"net/http"
	"os"
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
)

func initProducer()(sarama.SyncProducer, error) {

	keypair, err := tls.LoadX509KeyPair("C:\\Users\\sdd\\Downloads\\_VPN_KEYS\\Kafka\\service.cert",
		"C:\\Users\\sdd\\Downloads\\_VPN_KEYS\\Kafka\\service.key")
	if err != nil {
		log.Println(err)
		return nil, err
	}

	caCert, err := ioutil.ReadFile("C:\\Users\\sdd\\Downloads\\_VPN_KEYS\\Kafka\\ca.pem")
	if err != nil {
		log.Println(err)
		return nil, err
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
	brokers := []string{"my-kafka-service-my-aiven-project.aivencloud.com:12233"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	return producer, err
}

func publish(message string, producer sarama.SyncProducer) {
	// publish sync
	msg := &sarama.ProducerMessage {
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	// publish async
	//producer.Input() <- &sarama.ProducerMessage{

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
}

func main() {

	// create producer
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	publish("HI! Kafka", producer)






	redisClient := util.InitRedis()
	key1 := "time."+time.Now().Format("20060102.150405.000000000")
	value1 := &LogEntry{
		Operation:  "Update",
		AppEntity:  "Account",
		EntityName: "Jenny",
		CreateDate: primitive.NewDateTimeFromTime(time.Now()),
	}

	err = redisClient.SetKey(key1, value1, time.Hour*3000)
	if err != nil {
		log.Fatalf("Error: %v", err.Error())
	}

	value2 := &LogEntry{}
	err = redisClient.GetKey(key1, value2)
	if err != nil {
		log.Fatalf("Error: %v", err.Error())
	}

	log.Printf("Update: %s", value2.AppEntity)
	log.Printf("Email: %s", value2.EntityName)

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
