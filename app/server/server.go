package server

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/go-pkgz/rest"
	"github.com/go-pkgz/rest/logger"
)

// StatRecord for stats
type StatRecord struct {
	ID       string     `json:"id,omitempty"`
	FromIP   string     `json:"from_ip"`
	TS       *time.Time `json:"ts,omitempty"`
	Fname    string     `json:"fname"`
	DestHost string     `json:"dest"`
}

// Render for StatRecord
func (s *StatRecord) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}

// RLBStatsServer - rlb-stats server
type RLBStatsServer struct {
	version string
	port    int
}

// NewRLBStatsServer makes new rlb-stats server
func NewRLBStatsServer(version string, port int) *RLBStatsServer {
	server := RLBStatsServer{
		version: version,
		port:    port,
	}
	return &server
}

// Run activates webserver
func (s *RLBStatsServer) Run() {
	log.Printf("[INFO] activate web server on port %d", s.port)
	r := chi.NewRouter()

	r.Use(middleware.RequestID, middleware.RealIP, rest.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(rest.AppInfo("RLB-Stats", "alaija", s.version), rest.Ping)

	l := logger.New(logger.Flags(logger.All), logger.Prefix("[INFO]"))
	r.Use(l.Handler)

	r.Route("/", func(r chi.Router) {
		r.Post("/", s.processStats)
	})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", s.port), r))
}

func (s *RLBStatsServer) processStats(w http.ResponseWriter, r *http.Request) {
	var record StatRecord
	if err := render.DecodeJSON(r.Body, &record); err != nil {
		rest.SendErrorJSON(w, r, http.StatusBadRequest, err, "Bad Request")
		return
	}
	render.Render(w, r, &record)
}
