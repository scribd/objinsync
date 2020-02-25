package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"git.lo/data-platform/objinsync/pkg/sync"
)

var (
	RunOnce            bool
	InitialRunFinished atomic.Bool
	HealthCheckAddr    = ":8087"

	metricsSyncTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "objinsync",
		Subsystem: "loop",
		Name:      "sync_time",
		Help:      "Number of milliseconds it takes to complete a full sync looop.",
	})
)

func init() {
	prometheus.MustRegister(metricsSyncTime)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if InitialRunFinished.Load() {
		fmt.Fprintf(w, "GOOD")
	} else {
		http.Error(w, "Pull not finished", http.StatusInternalServerError)
	}
}

func serveHealthCheckEndpoints() {
	http.HandleFunc("/health", healthCheckHandler)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(HealthCheckAddr, nil))
}

func main() {
	if os.Getenv("DEBUG") != "" {
		logger, _ := zap.NewDevelopment()
		zap.ReplaceGlobals(logger)
	} else {
		logger, _ := zap.NewProduction()
		zap.ReplaceGlobals(logger)
	}
	l := zap.S()

	if os.Getenv("SENTRY_DSN") != "" {
		err := sentry.Init(sentry.ClientOptions{})
		if err != nil {
			l.Errorf("Sentry initialization failed: %v", err)
		} else {
			l.Infof("Initialized Sentry integration.")
			defer sentry.Flush(time.Second * 5)
			defer func() {
				// manually capture panic so we can do our own logging
				r := recover()
				if r != nil {
					fmt.Println(r, string(debug.Stack()))
					defer sentry.Recover()
					panic(r)
				}
			}()
		}
	} else {
		l.Warnf("SENTRY_DSN not found, skipped Sentry setup.")
	}

	var rootCmd = &cobra.Command{
		Short: "Continously synchronize a remote object store directory with a local directory",
		Use:   "objinsync",
	}

	var pullCmd = &cobra.Command{
		Use:   "pull REMOTE_URI LOCAL_PATH",
		Args:  cobra.ExactArgs(2),
		Short: "Pull from remote to local",
		Run: func(cmd *cobra.Command, args []string) {
			remoteUri := args[0]
			localDir := args[1]
			interval := time.Second * 5

			_, err := os.Stat(localDir)
			if err != nil {
				log.Fatal(localDir, " is not a valid dir: ", err)
			}

			puller := sync.NewPuller()
			pull := func() {
				start := time.Now()
				l.Info("Pull started.")

				errMsg := puller.Pull(remoteUri, localDir)
				if errMsg != "" {
					sentry.CaptureMessage(errMsg)
					sentry.Flush(time.Second * 5)
					l.Fatalf(errMsg)
				}

				syncTime := time.Now().Sub(start)
				metricsSyncTime.Set(float64(syncTime / time.Millisecond))
				l.Infof("Pull finished in %v seconds.", syncTime)
			}

			if RunOnce {
				l.Infof("Pulling from %s to %s...", remoteUri, localDir)
				pull()
			} else {
				InitialRunFinished.Store(false)
				go serveHealthCheckEndpoints()
				l.Infof("Serving health check endpoints at: %s.", HealthCheckAddr)
				l.Infof("Pulling from %s to %s every %v...", remoteUri, localDir, interval)
				ticker := time.NewTicker(interval)
				pull()
				for {
					select {
					case <-ticker.C:
						pull()
						InitialRunFinished.Store(true)
					}
				}
			}
		},
	}
	pullCmd.PersistentFlags().BoolVarP(&RunOnce, "once", "o", false, "run action once and then exit.")

	rootCmd.AddCommand(pullCmd)
	rootCmd.Execute()
}
