package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/phyber/negroni-gzip/gzip"
	"github.com/rs/cors"
)

type statInfo struct {
	ID       string
	Platform string
	Product  string
	Stat     string
	Sub1     string
	Sub2     string
	Sub3     string
	Info     map[string]interface{}
}

type stat struct {
	Member string
	Value  string
	When   string
	Stat   statInfo
}

func getLatestUserStats(userid string) ([]stat, error) {
	var rval = []stat{}
	rows, err := lastestUserStats.Query(userToID.find(userid))
	if err != nil {
		return rval, err
	}
	defer rows.Close()
	for rows.Next() {
		var s = stat{}
		var info []byte
		err = rows.Scan(
			&s.Member,
			&s.Value,
			&s.Stat.ID,
			&s.Stat.Platform,
			&s.Stat.Product,
			&s.Stat.Stat,
			&s.Stat.Sub1,
			&s.Stat.Sub2,
			&s.Stat.Sub3,
			&info,
			&s.When)
		if err != nil {
			return rval, err
		}
		if err := json.Unmarshal(info, &s.Stat.Info); err != nil {
			s.Stat.Info = map[string]interface{}{}
		}
		rval = append(rval, s)
	}
	return rval, err
}

func handleUserJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/json")
	v := mux.Vars(r)
	if u, ok := v["userid"]; ok {
		if s, err := getLatestUserStats(u); err != nil {
			log.Println("error in handleUserJSON", err.Error())
		} else {
			json.NewEncoder(w).Encode(s)
		}
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, `
	/v1/u/{userid}.json		Latest daily stats for a user
	`)
}

func mindHTTP() {
	r := mux.NewRouter()
	r.HandleFunc("/v1/u/{userid}.json", handleUserJSON)
	r.HandleFunc("/v1/stats.json", handleStatsList)
	r.HandleFunc("/v1/hourly.{type}", handleHourlyJson)
	r.HandleFunc("/v1/hourly/sum.json", handleHourlySumJson)
	r.HandleFunc("/v1/help", handleIndex)
	n := negroni.New()
	n.Use(cors.New(cors.Options{AllowedOrigins: []string{"*"}}))
	n.Use(negroni.NewRecovery())
	n.Use(negroni.NewLogger())
	n.Use(gzip.Gzip(gzip.DefaultCompression))
	n.UseHandler(r)
	n.Run("0.0.0.0:8874")
}
