package main

import (
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/phyber/negroni-gzip/gzip"
	"github.com/rs/cors"
)

var (
	router = mux.NewRouter()
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

func mindHTTP() {
	n := negroni.New()
	n.Use(cors.New(cors.Options{AllowedOrigins: []string{"*"}}))
	n.Use(negroni.NewRecovery())
	n.Use(negroni.NewLogger())
	n.Use(gzip.Gzip(gzip.DefaultCompression))
	n.UseHandler(router)
	n.Run("0.0.0.0:8874")
}
