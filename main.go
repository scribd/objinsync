package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/scribd/objinsync/pkg/sync"
)

var (
	InitialRunFinished  atomic.Bool
	FlagRunOnce         bool
	FlagStatusAddr      = ":8087"
	FlagExclude         []string
	FlagScratch         bool
	FlagDefaultFileMode = "0664"
	FlagS3Endpoint      = ""
	FlagDisableSSL      = false

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
	log.Fatal(http.ListenAndServe(FlagStatusAddr, nil))
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
		l.Infof("SENTRY_DSN not found, sentry integration disabled.")
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

			puller, err := sync.NewPuller(remoteUri, localDir)
			if err != nil {
				log.Fatal(err)
			}
			puller.DisableSSL = FlagDisableSSL
			puller.S3Endpoint = FlagS3Endpoint
			if FlagExclude != nil {
				puller.AddExcludePatterns(FlagExclude)
			}
			if !FlagScratch {
				puller.PopulateChecksum()
			}
			if FlagDefaultFileMode != "" {
				mode, err := strconv.ParseInt(FlagDefaultFileMode, 8, 64)
				if err != nil {
					log.Fatal("invalid default file mode", err)
				}
				puller.SetDefaultFileMode(os.FileMode(mode))
			}

			pull := func() {
				start := time.Now()
				l.Info("Pull started.")

				errMsg := puller.Pull()
				if errMsg != "" {
					sentry.CaptureMessage(errMsg)
					sentry.Flush(time.Second * 5)
					fmt.Println("ERROR: failed to pull objects from remote store:", errMsg)
					os.Exit(1)
				}

				syncTime := time.Now().Sub(start)
				metricsSyncTime.Set(float64(syncTime / time.Millisecond))
				l.Infof("Pull finished in %v seconds.", syncTime)
			}

			if FlagRunOnce {
				l.Infof("Pulling from %s to %s...", remoteUri, localDir)
				pull()
			} else {
				InitialRunFinished.Store(false)
				go serveHealthCheckEndpoints()
				l.Infof("Serving health check endpoints at: %s.", FlagStatusAddr)
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

	pullCmd.PersistentFlags().BoolVarP(
		&FlagRunOnce, "once", "o", false, "run action once and then exit")
	pullCmd.PersistentFlags().BoolVarP(
		&FlagDisableSSL, "disable-ssl", "", false, "disable SSL for object storage connection")
	pullCmd.PersistentFlags().StringVarP(
		&FlagStatusAddr, "status-addr", "s", ":8087", "binding address for status endpoint")
	pullCmd.PersistentFlags().StringSliceVarP(
		&FlagExclude, "exclude", "e", nil, "exclude files matching given pattern, see https://github.com/bmatcuk/doublestar#patterns for pattern spec")
	pullCmd.PersistentFlags().BoolVarP(
		&FlagScratch,
		"scratch",
		"",
		false,
		"skip checksums calculation and override all files during the initial sync",
	)
	pullCmd.PersistentFlags().StringVarP(
		&FlagDefaultFileMode, "default-file-mode", "m", "0664", "default mode to use for creating local file")
	pullCmd.PersistentFlags().StringVarP(
		&FlagS3Endpoint, "s3-endpoint", "", "", "override endpoint to use for remote object store (e.g. minio)")

	rootCmd.AddCommand(pullCmd)
	rootCmd.Execute()
}
