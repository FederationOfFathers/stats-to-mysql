package main

import (
	"fmt"
	"net/http"
)

func init() {
	router.HandleFunc("/v1/help", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, `
	/v1/u/{userid}.json		Latest daily stats for a user
	`)
	})
}
