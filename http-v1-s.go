package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

func init() {
	router.Path("/v1/s/{statlist}.json").Handler(mw(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/json")
		v := mux.Vars(r)
		var rval = map[string]map[string]string{}

		for _, statID := range strings.Split(v["statlist"], ",") {
			s, err := db.Prepare("" +
				"SELECT" +
				"	slack,value " +
				"FROM " +
				"	stats_latest sl " +
				"	LEFT JOIN stats_hourly sh " +
				"		ON ( " +
				"			sl.member_id = sh.member_id AND " +
				"			sl.stat_id = sh.stat_id AND " +
				"			`when` = `hourly` " +
				"		) " +
				"	LEFT JOIN members m " +
				"		ON ( sl.member_id = id ) " +
				"WHERE " +
				"	sl.stat_id = ?")
			defer s.Close()
			if err != nil {
				log.Println("/v1/s/{statlist}.json error preparing query:", err)
				continue
			}
			rows, err := s.Query(statID)
			for rows.Next() {
				var name *string
				var value *string
				err := rows.Scan(&name, &value)
				if err != nil {
					log.Println("/v1/s/{statlist}.json error scannind results:", err)
					continue
				}
				if name == nil {
					continue
				}
				if _, ok := rval[*name]; !ok {
					rval[*name] = map[string]string{}
				}
				if value != nil {
					rval[*name][statID] = *value
				} else {
					rval[*name][statID] = ""
				}
			}
			rows.Close()
		}
		json.NewEncoder(w).Encode(rval)
	}))
}
