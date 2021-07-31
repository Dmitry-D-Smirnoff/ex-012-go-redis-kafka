package app

import (
	u "ex-012-go-redis-kafka/util"
	"net/http"
)

var NotFoundHandler = func(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		u.Respond(w, u.Message(false, "This resources was not found on our server"))
		next.ServeHTTP(w, r)
	})
}

var HandleNotFound = func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	u.Respond(w, u.Message(false, "This resources was not found on our server"))
}

