package main

import (
	"net/http"
	"os"

	"github.com/FederationOfFathers/consul"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

var (
	router   = mux.NewRouter()
	listenOn = "0.0.0.0:8874"
	logger   = func() *zap.Logger {
		l, _ := zap.NewDevelopment()
		return l.With(zap.String("module", "mysql-to-stats"))
	}()
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
	return http.HandlerFunc(fn)
}

func mindHTTP() {
	if err := consul.RegisterOn("stats-to-mysql", listenOn); err != nil {
		panic(err)
	}
	logger.Fatal(
		"error starting API http server",
		zap.String("listenOn", listenOn),
		zap.Error(
			http.ListenAndServe(
				listenOn,
				handlers.CORS(
					handlers.AllowedOrigins([]string{"*"}),
				)(
					handlers.CombinedLoggingHandler(
						os.Stdout,
						handlers.CompressHandler(
							router,
						),
					),
				),
			),
		),
	)
}
