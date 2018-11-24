package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/go-pkgz/rest"
	"github.com/go-pkgz/rest/logger"
)

// RLBStatsServer - rlb-stats server
type RLBStatsServer struct {
	version string
	port    int

	httpServer *http.Server
	lock       sync.Mutex
}

// NewRLBStatsServer makes new rlb-stats server
func NewRLBStatsServer(version string, port int) *RLBStatsServer {
	server := RLBStatsServer{
		version: version,
		port:    port,
	}

	return &server
}

// Run activates rlb-stats rest server
func (s *RLBStatsServer) Run() {
	log.Printf("[INFO] activate web server on port %d", s.port)
	router := s.routes()

	s.lock.Lock()
	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	s.lock.Unlock()

	err := s.httpServer.ListenAndServe()
	log.Printf("[WARN] http server terminated, %s", err)
}

// Shutdown rlb-stats rest server
func (s *RLBStatsServer) Shutdown() {
	log.Print("[WARN] shutdown rest server")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s.lock.Lock()
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Printf("[DEBUG] http shutdown error, %s", err)
		}
		log.Print("[DEBUG] shutdown http server completed")
	}
	s.lock.Unlock()
}

func (s *RLBStatsServer) routes() chi.Router {
	router := chi.NewRouter()

	router.Use(middleware.RequestID, middleware.RealIP, rest.Recoverer)
	router.Use(middleware.Timeout(60 * time.Second))
	router.Use(rest.AppInfo("RLB-Stats", "alaija", s.version), rest.Ping)

	l := logger.New(logger.Flags(logger.All), logger.Prefix("[INFO]"))
	router.Use(l.Handler)

	router.Post("/stats", s.processStats)

	return router
}

//LogRecord is reference to incomming request sent by RLB (see https://github.com/umputun/rlb#stats)
type LogRecord struct {
	ID       string    `json:"id,omitempty"`
	FromIP   string    `json:"from_ip"`
	TS       time.Time `json:"ts,omitempty"`
	Fname    string    `json:"fname"`
	DestHost string    `json:"dest"`
}

func (s *RLBStatsServer) processStats(w http.ResponseWriter, r *http.Request) {
	record := LogRecord{}

	if err := render.DecodeJSON(r.Body, &record); err != nil {
		rest.SendErrorJSON(w, r, http.StatusBadRequest, err, "Bad Request")
		return
	}

	render.JSON(w, r, &record)
}
