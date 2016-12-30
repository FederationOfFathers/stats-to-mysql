package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func init() {
	router.Path("/v1/u/{userid}.json").Handler(mw(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/json")
		v := mux.Vars(r)
		if u, ok := v["userid"]; ok {
			if s, err := getLatestUserStats(u); err != nil {
				log.Println("error in handleUserJSON", err.Error())
			} else {
				json.NewEncoder(w).Encode(s)
			}
		}
	}))
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
