package main

import (
	"net/http"

	"github.com/FederationOfFathers/consul"
	"github.com/NYTimes/gziphandler"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	nocache "github.com/rabeesh/negroni-nocache"
	"github.com/rs/cors"
	"github.com/uber-go/zap"
)

var (
	router   = mux.NewRouter()
	listenOn = "0.0.0.0:8874"
	logger   = zap.New(zap.NewJSONEncoder()).With(zap.String("module", "mysql-to-stats"))
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

func mw(fn func(w http.ResponseWriter, r *http.Request)) http.Handler {
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:*", "http://127.0.0.*"},
		AllowCredentials: true,
	})
	return gziphandler.GzipHandler(
		c.Handler(
			negroni.New(
				&httpLogger{},
				negroni.NewRecovery(),
				nocache.New(true),
				negroni.Wrap(
					http.HandlerFunc(fn),
				),
			),
		),
	)
}

func mindHTTP() {
	if err := consul.RegisterListenOn("stats-to-mysql", listenOn); err != nil {
		panic(err)
	}
	logger.Fatal(
		"error starting API http server",
		zap.String("listenOn", listenOn),
		zap.Error(http.ListenAndServe(listenOn, router)))
}
