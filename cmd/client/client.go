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
	"sync/atomic"
	"time"

	"bitbucket.org/hnakamur/ws_surveyor/msg"
	"gopkg.in/vmihailenco/msgpack.v2"

	"github.com/gorilla/websocket"
	"github.com/hnakamur/ltsvlog"
)

var addr = flag.String("addr", "localhost:8080", "http service address")
var clientID = flag.String("client-id", "client1", "client ID")
var minDelay = flag.Duration("min-delay", 3*time.Second, "min delay")
var maxDelay = flag.Duration("max-delay", 5*time.Second, "max delay")

func randomDelay() time.Duration {
	return time.Duration(int64(*minDelay) + rand.Int63n(int64(*maxDelay)-int64(*minDelay)))
}

func main() {
	flag.Parse()

	rand.Seed(int64(time.Now().Unix()))

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	ltsvlog.Logger.Info(ltsvlog.LV{"msg", "connecting to server"}, ltsvlog.LV{"address", u.String()})

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "dial error"},
			ltsvlog.LV{"address", u.String()},
			ltsvlog.LV{"err", err},
		)
		os.Exit(1)
	}
	defer c.Close()

	done := make(chan struct{})

	go func() {
		defer c.Close()
		defer close(done)
		for {
			wsMsgType, r, err := c.NextReader()
			if err != nil {
				ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "read error"},
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
			case msg.JobResultsMsg:
				var jobResults msg.JobResults
				err := dec.Decode(&jobResults)
				if err != nil {
					ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
						ltsvlog.LV{"err", err})
					return
				}

				ltsvlog.Logger.Info(ltsvlog.LV{"msg", "received JobResults"},
					ltsvlog.LV{"jobResults", jobResults})
			case msg.JobMsg:
				var job msg.Job
				err := dec.Decode(&job)
				if err != nil {
					ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
						ltsvlog.LV{"err", err})
					return
				}

				if ltsvlog.Logger.DebugEnabled() {
					ltsvlog.Logger.Debug(ltsvlog.LV{"msg", "ignore received Job"},
						ltsvlog.LV{"job", job})
				}
			default:
				ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
					ltsvlog.LV{"messageType", msgType})
				return
			}
		}
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()
	var jobID uint64

	for {
		select {
		case <-timer.C:
			job := msg.Job{
				JobID:   atomic.AddUint64(&jobID, 1),
				Targets: []string{"target1", "target2"},
			}
			b, err := msgpack.Marshal(msg.JobMsg, &job)
			if err != nil {
				ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
					ltsvlog.LV{"job", job},
					ltsvlog.LV{"err", err})
				return
			}
			err = c.WriteMessage(websocket.BinaryMessage, b)
			if err != nil {
				ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "write error"},
					ltsvlog.LV{"job", job},
					ltsvlog.LV{"err", err})
				return
			}
			timer.Reset(randomDelay())

		case <-interrupt:
			ltsvlog.Logger.Info(ltsvlog.LV{"msg", "interrupt"})
			// To cleanly close a connection, a client should send a close
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
			c.Close()
			return
		}
	}
}
