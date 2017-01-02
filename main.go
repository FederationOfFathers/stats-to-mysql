package main

import (
	"github.com/apokalyptik/cfg"
	"github.com/hashicorp/consul/api"
)

var consul *api.Client

func mustConsul() {
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		panic(err)
	}
	consul = client
}

func main() {
	mustConsul()
	cfg.Parse()
	initMySQL()
	initNSQ()
	defer db.Close()
	mindHTTP()
}
