// Copyright 2015 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"bitbucket.org/hnakamur/ws_surveyor/msg"
	"gopkg.in/vmihailenco/msgpack.v2"

	"github.com/gorilla/websocket"
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

	rand.Seed(int64(time.Now().Unix()))

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	header := map[string][]string{
		"X-Worker-ID": []string{*workerID},
	}
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	ltsvlog.Logger.Info(ltsvlog.LV{"msg", "connecting to server"}, ltsvlog.LV{"address", u.String()})

	for {
		done := make(chan struct{})
		send := make(chan []byte, 256)
		var err error

		c, _, err := websocket.DefaultDialer.Dial(u.String(), header)
		if err != nil {
			ltsvlog.Logger.Error(ltsvlog.LV{"msg", "dial error"},
				ltsvlog.LV{"address", u.String()},
				ltsvlog.LV{"err", err},
			)
			goto retry_connect
		}
		defer c.Close()

		go func() {
			defer close(done)
			for {
				wsMsgType, r, err := c.NextReader()
				if err != nil {
					ltsvlog.Logger.Error(ltsvlog.LV{"msg", "read error"},
						ltsvlog.LV{"err", err})
					return
				}
				if wsMsgType != websocket.BinaryMessage {
					ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected wsMsgType"},
						ltsvlog.LV{"wsMsgType", wsMsgType})
					return
				}
				dec := msgpack.NewDecoder(r)
				var msgType msg.MessageType
				err = dec.Decode(&msgType)
				if err != nil {
					ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
						ltsvlog.LV{"err", err})
					return
				}
				switch msgType {
				case msg.RegisterWorkerResultMsg:
					var registerWorkerResult msg.RegisterWorkerResult
					err := dec.Decode(&registerWorkerResult)
					if err != nil {
						ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
							ltsvlog.LV{"err", err})
						return
					}
					if registerWorkerResult.Error != "" {
						ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to register worker"},
							ltsvlog.LV{"workerID", *workerID},
							ltsvlog.LV{"err", registerWorkerResult.Error})
						os.Exit(1)
					}
					ltsvlog.Logger.Info(ltsvlog.LV{"msg", "registered myself as worker"},
						ltsvlog.LV{"workerID", *workerID})
				case msg.JobMsg:
					var job msg.Job
					err := dec.Decode(&job)
					if err != nil {
						ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
							ltsvlog.LV{"err", err})
						return
					}

					if ltsvlog.Logger.DebugEnabled() {
						ltsvlog.Logger.Debug(ltsvlog.LV{"msg", "received Job"},
							ltsvlog.LV{"workerID", *workerID},
							ltsvlog.LV{"job", job})
					}
					go func() {
						time.Sleep(randomDelay())

						// NOTE: Do some work.
						params := job.Params.(map[interface{}]interface{})
						targets := strings.Split(params["targets"].(string), ",")
						results := make(map[string]bool)
						jobResult := msg.WorkerResult{
							JobID: job.ID,
							Data:  results,
						}
						for _, target := range targets {
							results[target] = true
						}

						b, err := msgpack.Marshal(msg.WorkerResultMsg, &jobResult)
						if err != nil {
							ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
								ltsvlog.LV{"jobResult", jobResult},
								ltsvlog.LV{"err", err})
							return
						}
						send <- b
					}()
				default:
					ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
						ltsvlog.LV{"messageType", msgType})
					return
				}
			}
		}()

		for {
			select {
			case b := <-send:
				err = c.WriteMessage(websocket.BinaryMessage, b)
				if err != nil {
					ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "write error"},
						ltsvlog.LV{"err", err})
					goto retry_connect
				}
			case <-done:
				if ltsvlog.Logger.DebugEnabled() {
					ltsvlog.Logger.Debug(ltsvlog.LV{"msg", "received done"})
				}
				goto retry_connect

			case <-interrupt:
				ltsvlog.Logger.Info(ltsvlog.LV{"msg", "interrupt"})
				// To cleanly close a connection, a worker should send a close
				// frame and wait for the server to close the connection.
				err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				if err != nil {
					ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "write close error"},
						ltsvlog.LV{"err", err})
					return
				}
				select {
				case <-done:
				case <-time.After(time.Second):
				}
				return
			}
		}

	retry_connect:
		select {
		case <-interrupt:
			ltsvlog.Logger.Info(ltsvlog.LV{"msg", "interrupt in retry_eonnect"})
			return
		case <-time.After(time.Second):
			if ltsvlog.Logger.DebugEnabled() {
				ltsvlog.Logger.Debug(ltsvlog.LV{"msg", "retrying connect to server"})
			}
		}
	}
}
