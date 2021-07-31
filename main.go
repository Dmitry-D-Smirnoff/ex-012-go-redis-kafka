package main

import (
	"ex-012-go-redis-kafka/app"
	"ex-012-go-redis-kafka/controller"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"os"
)

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/api/log/recent/{limit}", controller.GetLastLogEntries).Methods("GET")
	router.NotFoundHandler = http.HandlerFunc(app.HandleNotFound)

	port := os.Getenv("PORT") //Получить порт из файла .env; мы не указали порт, поэтому при локальном тестировании должна возвращаться пустая строка
	if port == "" {
		port = "8000" //localhost
	}
	err := http.ListenAndServe(":" + port, router) //Запустите приложение, посетите localhost:8000/api
	if err != nil {
		fmt.Print(err)
	}
}