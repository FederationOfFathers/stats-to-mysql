package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

func init() {
	router.Path("/v1/daily.json").Handler(mw(func(w http.ResponseWriter, r *http.Request) {
		users := strings.Split(r.URL.Query().Get("users"), ",")
		if len(users) < 1 {
			return
		}
		stats := strings.Split(r.URL.Query().Get("stats"), ",")
		if len(stats) < 1 {
			return
		}
		last, err := strconv.Atoi(r.URL.Query().Get("last"))
		if err != nil {
			last = -1
		}
		var rval = map[string]map[string]map[string]int{}
		//             userid     stat   date   stat
		for _, u := range users {
			rval[u] = map[string]map[string]int{}
			//            stat       date   stat
			for _, s := range stats {
				rval[u][s], _ = getUserDailyStats(u, s, last)
			}
		}
		json.NewEncoder(w).Encode(rval)
	}))
}

func getUserDailyStats(user, stat string, last int) (map[string]int, error) {
	var rval = map[string]int{}
	var err error
	var rows *sql.Rows
	var statID int
	statID, err = strconv.Atoi(stat)
	if err != nil {
		return rval, err
	}
	if last >= 0 {
		rows, err = db.Query(
			"SELECT `when`,`value` FROM stats_daily WHERE `member_id`=? AND `stat_id`=? AND `when` >= DATE_SUB(NOW(), INTERVAL ? DAY)",
			userToID.find(user),
			statID,
			last)
	} else {
		rows, err = db.Query(
			"SELECT `when`,`value` FROM stats_daily WHERE `member_id`=? AND `stat_id`=?",
			userToID.find(user),
			statID)
	}
	if err != nil {
		return rval, err
	}
	defer rows.Close()
	for rows.Next() {
		var when string
		var value int
		err = rows.Scan(&when, &value)
		if err != nil {
			break
		}
		rval[when] = value
	}
	return rval, err
}
