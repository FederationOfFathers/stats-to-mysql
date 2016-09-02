package main

import (
	"log"
	"sync"
)

var userToID = userLookup{}
var userLookupLock sync.RWMutex

type userLookup map[string]string

func (u userLookup) find(userID string) string {
	userLookupLock.RLock()
	memberID, ok := u[userID]
	userLookupLock.RUnlock()
	if ok {
		return memberID
	}

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
				userLookupLock.Lock()
				userToID[userID] = memberID
				userLookupLock.Unlock()
				log.Println("found ID for", userID, "=", memberID)
			}
		}
	}
	rows.Close()
	return memberID
}
