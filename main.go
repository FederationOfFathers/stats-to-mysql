package main

import (
	"github.com/FederationOfFathers/consul"
	"github.com/apokalyptik/cfg"
)

func main() {
	consul.Must()
	cfg.Parse()
	initMySQL()
	initNSQ()
	defer db.Close()
	mindHTTP()
}
