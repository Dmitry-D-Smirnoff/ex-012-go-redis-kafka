package main

import (
	"ex-012-go-redis-kafka/data"
	"ex-012-go-redis-kafka/util"
	"fmt"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"net/http"
	"os"
	"time"
)

func main() {

	data.ConnectMongoDB()
	defer data.DisconnectMongoDB()

	logs := []data.LogEntry{
		{"Log Scan", "Test", "Test", primitive.NewDateTimeFromTime(time.Now())},
		{"Log Scan", "Test", "Test", primitive.NewDateTimeFromTime(time.Now())},
		{"Log Scan", "Test", "Test", primitive.NewDateTimeFromTime(time.Now())},
		{"Log Scan", "Test", "Test", primitive.NewDateTimeFromTime(time.Now())},
		{"Log Scan", "Test", "Test", primitive.NewDateTimeFromTime(time.Now())},
	}
	data.InsertManyLogEntries(logs)


	redisClient := data.InitRedis()
	// create producer
	producer, consumer, err := data.InitProducer()
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
	go data.InitProducer()
	go data.Consumer([]string{"likes", "upload"})

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
