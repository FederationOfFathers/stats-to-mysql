package main

import "log"

var userToID = userLookup{}

type userLookup map[string]string

func (u userLookup) find(userID string) string {
	memberID, ok := u[userID]
	if !ok {
		_, err := userAdd.Exec(userID)
		if err != nil {
			log.Println("Error adding user", userID)
		}
		rows, err := userGet.Query(userID)
		if err != nil {
			log.Fatal("Error looking up user", userID)
		} else {
			for rows.Next() {
				err := rows.Scan(&memberID)
				if err != nil {
					log.Println("Error scanning user lookup result into memberID", err)
				} else {
					userToID[userID] = memberID
					log.Println("found ID for", userID, "=", memberID)
				}
			}
		}
		rows.Close()
	}
	return memberID
}
