package main

import (
	"flag"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"bitbucket.org/hnakamur/ws_surveyor/worker"

	"golang.org/x/net/context"

	"github.com/hnakamur/ltsvlog"
)

var addr = flag.String("addr", "localhost:8080", "http service address")
var workerID = flag.String("id", "worker1", "worker ID")
var minDelay = flag.Duration("min-delay", 1*time.Second, "min delay")
var maxDelay = flag.Duration("max-delay", 5*time.Second, "max delay")

func randomDelay() time.Duration {
	return time.Duration(int64(*minDelay) + rand.Int63n(int64(*maxDelay)-int64(*minDelay)))
}

func main() {
	flag.Parse()
	if *workerID == "" {
		ltsvlog.Logger.Error(ltsvlog.LV{"msg", "worker must not be empty"})
		os.Exit(1)
	}

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			<-interrupt
			cancel()
		}
	}()

	rand.Seed(int64(time.Now().Unix()))
	workFunc := func(params interface{}) interface{} {
		time.Sleep(randomDelay())

		// NOTE: Do some work.
		p := params.(map[interface{}]interface{})
		targets := strings.Split(p["targets"].(string), ",")
		results := make(map[string]bool)
		for _, target := range targets {
			results[target] = true
		}
		return results
	}
	w := worker.NewWorker(u, "X-Worker-ID", *workerID, 256, workFunc, time.Second, time.Second)
	err := w.Run(ctx)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "error from Run"},
			ltsvlog.LV{"err", err})
	}
}
