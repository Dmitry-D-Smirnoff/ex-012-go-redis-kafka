package main

import (
	"ex-012-go-redis-kafka/data"
	"ex-012-go-redis-kafka/util"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func main() {

	data.ConnectMongoDB()
	defer data.DisconnectMongoDB()
	redisClient := data.InitRedis()

	producer, consumer, err := data.InitKafka()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}
	util.SetProducer(producer)

	messageChannel := data.Consume(consumer)
	processingChannels := make([]chan *data.LogEntry, 3)
	for i, _ := range processingChannels{
		processingChannels[i] = make(chan *data.LogEntry)
		go data.ProcessMessages(redisClient, messageChannel, processingChannels[i], i)
	}

	go data.FinishProcessing(data.Merge(processingChannels))

	router := mux.NewRouter()
	router.HandleFunc("/api/log/new", util.GetLogNew).Methods("GET")
	router.NotFoundHandler = http.HandlerFunc(util.HandleNotFound)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8012" //localhost
	}
	err = http.ListenAndServe(":" + port, router)
	if err != nil {
		fmt.Print(err)
	}

}
