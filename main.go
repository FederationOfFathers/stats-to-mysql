package main

import (
	"github.com/apokalyptik/cfg"
)

func main() {
	cfg.Parse()
	initMySQL()
	initNSQ()
	defer db.Close()
	mindHTTP()
}
