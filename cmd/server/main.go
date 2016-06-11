// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"strconv"

	"bitbucket.org/hnakamur/ws_surveyor"
	"bitbucket.org/hnakamur/ws_surveyor/msg"

	"github.com/gorilla/websocket"
	"github.com/hnakamur/ltsvlog"
)

type workRequest struct {
	Params interface{} `json:"params"`
}

type workResponse struct {
	JobID   string                 `json:"job_id"`
	Results map[string]interface{} `json:"results"`
}

func newWorkResponse(jobID msg.JobID, results map[string]interface{}) *workResponse {
	workRes := &workResponse{
		JobID:   strconv.FormatUint(uint64(jobID), 10),
		Results: make(map[string]interface{}),
	}
	for workerID, r := range results {
		data := make(map[string]bool)
		for k, v := range r.(map[interface{}]interface{}) {
			data[k.(string)] = v.(bool)
		}
		workRes.Results[workerID] = data
	}
	return workRes
}

func serveWorkFunc(hub *ws_surveyor.Hub) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/work" {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		if r.Method != "POST" {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		dec := json.NewDecoder(r.Body)
		var v workRequest
		err := dec.Decode(&v)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		results, jobID, err := hub.RequestWork(v.Params)
		if err != nil {
			ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "error returned from worker hub"},
				ltsvlog.LV{"err", err})
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		workResp := newWorkResponse(jobID, results)
		enc := json.NewEncoder(w)
		err = enc.Encode(&workResp)
		if err != nil {
			ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to encode work response"},
				ltsvlog.LV{"err", err})
		}
	}
}

// ServeWS returns a function for handling websocket request from the peer.
func serveWSFunc(hub *ws_surveyor.Hub) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		workerID := r.Header.Get("X-Worker-ID")
		if workerID == "" {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		upgrader := websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to upgrade to webscoket"},
				ltsvlog.LV{"err", err})
			return
		}
		conn := ws_surveyor.NewConn(hub, ws, workerID, 256)
		err = conn.RegisterToHub()
		if err != nil {
			ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to register connection to hub"},
				ltsvlog.LV{"err", err})
			return
		}

		conn.Run()
	}
}

var addr = flag.String("addr", ":8080", "http service address")

func main() {
	flag.Parse()

	hub := ws_surveyor.NewHub()
	go hub.Run()
	http.HandleFunc("/work", serveWorkFunc(hub))
	http.HandleFunc("/ws", serveWSFunc(hub))
	ltsvlog.Logger.Info(ltsvlog.LV{"msg", "server start listening"}, ltsvlog.LV{"address", *addr})
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to listen"},
			ltsvlog.LV{"address", *addr},
			ltsvlog.LV{"err", err})
		os.Exit(1)
	}
}
