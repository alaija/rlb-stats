package main

import (
	"log"
	"os"

	"github.com/hashicorp/logutils"
	"github.com/jessevdk/go-flags"

	"github.com/alaija/rlb-stats/app/server"
)

var opts struct {
	Port int  `short:"p" long:"port" env:"PORT" default:"7070" description:"port"`
	Dbg  bool `long:"dbg" env:"DEBUG" description:"debug mode"`
}

var revision string

func main() {
	log.Printf("RLB-stats - %s", revision)

	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(1)
	}

	setupLog(opts.Dbg)

	server.NewRLBStatsServer(revision, opts.Port).Run()
}

func setupLog(dbg bool) {
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel("INFO"),
		Writer:   os.Stdout,
	}

	log.SetFlags(log.Ldate | log.Ltime)

	if dbg {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
		filter.MinLevel = logutils.LogLevel("DEBUG")
	}

	log.SetOutput(filter)
}
