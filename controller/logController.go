package controller

import (
	u "ex-012-go-redis-kafka/util"
	"net/http"
)

var GetLastLogEntries = func(w http.ResponseWriter, r *http.Request) {

	resp := u.Message(true, "success")
	u.Respond(w, resp)
}

