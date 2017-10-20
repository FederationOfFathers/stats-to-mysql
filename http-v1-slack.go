package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

func init() {
	router.Path("/v1/slack/activity/last-{days}.json").Handler(mw(func(w http.ResponseWriter, r *http.Request) {
		v := mux.Vars(r)
		daysString := v["days"]
		days, _ := strconv.Atoi(daysString)
		w.Header().Set("Content-Type", "text/json")
		var list, err = getStatsList()
		if err != nil {
			log.Println("error getting stats list:", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var in = []string{}
		var st = map[int]statDesc{}

		for _, v := range list {
			if v.Platform != "slack" {
				continue
			}
			if v.Product != "slack" {
				continue
			}
			if v.Stat != "messages" {
				continue
			}
			if v.Sub2 == "" {
				continue
			}
			var id = strconv.Itoa(v.ID)
			st[v.ID] = v
			in = append(in, id)
		}

		var inSQL = strings.Join(in, ",")
		query := fmt.Sprintf(
			"SELECT stat_id,`when`,SUM(`value`) FROM `stats_daily` WHERE `stat_id` IN(%s) AND `when` > DATE_SUB(NOW(), INTERVAL ? DAY) GROUP BY `stat_id`,`when` ORDER BY `when` DESC",
			inSQL,
		)
		rows, err := db.Query(query, days)
		if err != nil {
			log.Printf("error querying: %s: %s", query, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var rval = map[string]map[time.Time]int{}

		for rows.Next() {
			var id int
			var when time.Time
			var sum int
			err := rows.Scan(&id, &when, &sum)
			if err != nil {
				log.Printf("error scanning: %s", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var stat = st[id]
			if _, ok := rval[stat.Sub2]; !ok {
				rval[stat.Sub2] = map[time.Time]int{}
			}
			rval[stat.Sub2][when] = sum
		}

		t, _ := time.Parse("2006-01-02", time.Now().Format("2006-01-02"))
		dates := []time.Time{}
		for i := 0; i < days; i++ {
			dates = append(dates, t.Add(0-time.Hour*24*time.Duration(i)))
		}

		for stat, times := range rval {
			for t := range dates {
				if _, ok := times[dates[t]]; !ok {
					rval[stat][dates[t]] = 0
				}
			}
		}
		json.NewEncoder(w).Encode(rval)
	}))
}
