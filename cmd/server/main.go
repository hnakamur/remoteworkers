// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"text/template"

	"github.com/hnakamur/ltsvlog"
)

var addr = flag.String("addr", ":8080", "http service address")
var homeTemplate = template.Must(template.ParseFiles("home.html"))

func serveHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, "Not found", 404)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	homeTemplate.Execute(w, r.Host)
}

func main() {
	flag.Parse()
	go hub.run()
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/work", serveWork)
	http.HandleFunc("/ws", serveWs)
	fmt.Printf("server start listening %s...\n", *addr)
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to listen"},
			ltsvlog.LV{"address", *addr},
			ltsvlog.LV{"err", err})
		os.Exit(1)
	}
}
