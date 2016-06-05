// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"sync/atomic"

	"github.com/hnakamur/ltsvlog"
	"gopkg.in/vmihailenco/msgpack.v2"

	"bitbucket.org/hnakamur/ws_surveyor/msg"
)

// hub maintains the set of active connections and broadcasts messages to the
// connections.
type Hub struct {
	// Registered connections.
	connections map[*Conn]bool

	// Inbound messages from the connections.
	broadcast chan []byte

	// Register requests from the connections.
	register chan *Conn

	// Unregister requests from connections.
	unregister chan *Conn

	// Registered workers.
	workers map[uint64]*Conn

	// Register worker requests from connections.
	registerWorker chan *Conn

	// Register worker results to connections.
	registerWorkerResult chan bool

	// Inbound messages from the connections.
	broadcastToWorkers chan jobRequest

	// working job results
	jobResults map[uint64]*jobResultsBuffer

	jobResultOrErrorC chan jobResultOrError
}

var hub = Hub{
	broadcast:            make(chan []byte),
	register:             make(chan *Conn),
	unregister:           make(chan *Conn),
	connections:          make(map[*Conn]bool),
	registerWorker:       make(chan *Conn),
	registerWorkerResult: make(chan bool),
	workers:              make(map[uint64]*Conn),
	broadcastToWorkers:   make(chan jobRequest),
	jobResults:           make(map[uint64]*jobResultsBuffer),
	jobResultOrErrorC:    make(chan jobResultOrError),
}

type jobRequest struct {
	conn *Conn
	job  msg.Job
}

type jobResultsBuffer struct {
	conn    *Conn
	results map[uint64]*msg.JobResult
}

func (b *jobResultsBuffer) gotAllResults() bool {
	for _, r := range b.results {
		if r == nil {
			return false
		}
	}
	return true
}

func (b *jobResultsBuffer) JobResults() msg.JobResults {
	r := msg.JobResults{
		Results: make([]msg.JobResult, 0, len(b.results)),
	}
	for _, br := range b.results {
		r.JobID = br.JobID
		r.Results = append(r.Results, *br)
	}
	return r
}

func newJobResultsBuffer(c *Conn) *jobResultsBuffer {
	return &jobResultsBuffer{
		conn:    c,
		results: make(map[uint64]*msg.JobResult),
	}
}

type jobResultOrError struct {
	result *msg.JobResult
	err    error
}

func (h *Hub) run() {
	for {
		select {
		case conn := <-h.register:
			h.connections[conn] = true
		case conn := <-h.unregister:
			if _, ok := h.connections[conn]; ok {
				delete(h.connections, conn)
				close(conn.send)
			}
		case message := <-h.broadcast:
			for conn := range h.connections {
				select {
				case conn.send <- message:
				default:
					close(conn.send)
					delete(hub.connections, conn)
				}
			}
		case conn := <-h.registerWorker:
			workerID := atomic.LoadUint64(&conn.workerID)
			_, exists := h.workers[workerID]
			if exists {
				h.registerWorkerResult <- false
				continue
			}
			h.workers[workerID] = conn
			h.registerWorkerResult <- true
		case req := <-h.broadcastToWorkers:
			job := req.job
			message, err := msgpack.Marshal(msg.JobMsg, &job)
			if err != nil {
				ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
					ltsvlog.LV{"job", job},
					ltsvlog.LV{"err", err})
				return
			}
			resultsBuf := newJobResultsBuffer(req.conn)
			for workerID, conn := range h.workers {
				select {
				case conn.send <- message:
					resultsBuf.results[workerID] = nil
				default:
					close(conn.send)
					delete(hub.connections, conn)
				}
			}
			if len(resultsBuf.results) > 0 {
				h.jobResults[job.JobID] = resultsBuf
			}
		case roe := <-h.jobResultOrErrorC:
			if roe.err != nil {
				//TODO: error handling
			} else {
				res := roe.result
				resultsBuf := h.jobResults[res.JobID]
				resultsBuf.results[res.WorkerID] = res
				if resultsBuf.gotAllResults() {
					jobResults := resultsBuf.JobResults()
					message, err := msgpack.Marshal(msg.JobResultsMsg, &jobResults)
					if err != nil {
						ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
							ltsvlog.LV{"jobResults", jobResults},
							ltsvlog.LV{"err", err})
						return
					}
					resultsBuf.conn.send <- message
					delete(h.jobResults, res.JobID)
				}
			}
		}
	}
}
