package main

import (
	"ex-012-go-redis-kafka/util"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"time"
)

type valueEx struct {
	Name  string
	Email string
}

func main() {
	redisClient := util.InitRedis()
	key1 := "key01"+time.Now().String()
	value1 := &valueEx{Name: "someName", Email: "someemail@abc.com"}
	err := redisClient.SetKey(key1, value1, time.Hour*3000)
	if err != nil {
		log.Fatalf("Error: %v", err.Error())
	}

	value2 := &valueEx{}
	err = redisClient.GetKey(key1, value2)
	if err != nil {
		log.Fatalf("Error: %v", err.Error())
	}

	log.Printf("Name: %s", value2.Name)
	log.Printf("Email: %s", value2.Email)

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
