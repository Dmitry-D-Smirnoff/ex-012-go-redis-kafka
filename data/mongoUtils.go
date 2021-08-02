package data

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strconv"
)

var mongoBufferSize int

var currentClient *mongo.Client

func GetMongoBufferSize() int{
	if mongoBufferSize == 0{
		size, err := strconv.Atoi(os.Getenv("mongo_buffer_size"))
		if err != nil{
			mongoBufferSize = 1
		}else{
			mongoBufferSize = size
		}
	}
	return mongoBufferSize
}

func GetLogCollection() (collection *mongo.Collection){
	if currentClient == nil {
		ConnectMongoDB()
	}else{
		//TODO: Ping and if fails -> Disconnect/Connect again
		//currentClient.Ping(context.TODO(),???)
	}
	//TODO: Environmental variables for Database and Log collection
	return currentClient.Database("ex-011-database").Collection("logEntries")
}

func ConnectMongoDB(){
	fmt.Println("Current MongoDB Connection: " + os.Getenv("mongodb_uri"))
	// Create client
	client, err := mongo.NewClient(options.Client().
		ApplyURI(os.Getenv("mongodb_uri")))
	if err != nil {
		fmt.Println(err)
	}
	// Create connect
	err = client.Connect(context.TODO())
	if err != nil {
		fmt.Println(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Connected to MongoDB!")

	currentClient = client
}

func DisconnectMongoDB(){
	err := currentClient.Disconnect(context.TODO())

	if err != nil {
		fmt.Println(err)
	}else{
		currentClient = nil
		fmt.Println("Connection to MongoDB closed.")
	}

}

func InsertManyLogEntries(logEntries []LogEntry){
	logInterface := make([]interface{}, len(logEntries))
	for i, v := range logEntries {
		logInterface[i] = v
	}

	insertManyResult, err := GetLogCollection().InsertMany(context.TODO(), logInterface)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("SUCCESS: Inserted %d documents in MongoDB\n", len(insertManyResult.InsertedIDs))
}

