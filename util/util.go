package util

import (
	"encoding/json"
	"net/http"
)

func Message(status bool, message string) (map[string]interface{}) {
	return map[string]interface{} {"status" : status, "message" : message}
}

func Respond(w http.ResponseWriter, data map[string] interface{})  {
	w.Header().Add("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

var GetLastLogEntries = func(w http.ResponseWriter, r *http.Request) {

	resp := Message(true, "success")
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