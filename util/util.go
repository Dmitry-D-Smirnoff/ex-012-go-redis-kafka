package util

import (
	"encoding/json"
	"ex-012-go-redis-kafka/data"
	"fmt"
	"github.com/Shopify/sarama"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"net/http"
	"time"
)

func Message(status bool, message string) (map[string]interface{}) {
	return map[string]interface{} {"status" : status, "message" : message}
}

func Respond(w http.ResponseWriter, data map[string] interface{})  {
	w.Header().Add("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func SetProducer(producer sarama.SyncProducer){
	currentProducer = producer
}
var currentProducer sarama.SyncProducer



var GetLogNew = func(w http.ResponseWriter, r *http.Request) {

	redisValue1 := data.LogEntry{
		Operation:  "Log Scan",
		AppEntity:  "All",
		EntityName: "Not Needed",
		CreateDate: primitive.NewDateTimeFromTime(time.Now()),
	}
	logNumber := data.Publish(redisValue1, currentProducer)

	resp := Message(true, fmt.Sprintf("successfully added to Kafka log #%d", logNumber))
	Respond(w, resp)
}

var NotFoundHandler = func(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		Respond(w, Message(false, "This resources was not found on our server"))
		next.ServeHTTP(w, r)
	})
}

var HandleNotFound = func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	Respond(w, Message(false, "This resources was not found on our server"))
}