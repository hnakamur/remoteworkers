// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"sort"
	"sync/atomic"

	"github.com/hnakamur/ltsvlog"
	"gopkg.in/vmihailenco/msgpack.v2"

	"bitbucket.org/hnakamur/ws_surveyor/msg"
)

// hub maintains the set of active connections and broadcasts messages to the
// connections.
type Hub struct {
	// Registered workers.
	workers map[string]*Conn

	// Register worker requests from connections.
	registerWorkerC chan registerWorkerRequest

	// Unregister worker requests from connections.
	unregisterWorkerC chan *Conn

	// Register worker results to connections.
	registerWorkerResult chan bool

	// Inbound messages from the connections.
	broadcastToWorkersC chan jobRequestToHub

	// worker results buffers
	workerResultsBuffers map[msg.JobID]*workerResultsBuffer

	// channel for worker result
	workerResultToHubC chan workerResult

	// job ID
	jobID uint64
}

var hub = Hub{
	registerWorkerC:   make(chan registerWorkerRequest),
	unregisterWorkerC: make(chan *Conn),
	workers:           make(map[string]*Conn),

	broadcastToWorkersC:  make(chan jobRequestToHub),
	workerResultsBuffers: make(map[msg.JobID]*workerResultsBuffer),
	workerResultToHubC:   make(chan workerResult),
}

type registerWorkerRequest struct {
	conn    *Conn
	resultC chan bool
}

type jobRequestToHub struct {
	job     msg.Job
	resultC chan jobResultOrError
}

type jobResult struct {
	results map[string]*workerResult
}

type jobResultOrError struct {
	result jobResult
	err    error
}

type workerResult struct {
	workerID string
	result   *msg.WorkerResult
}

type workerResultsBuffer struct {
	resultC chan jobResultOrError
	results map[string]*workerResult
}

func newWorkerResultsBuffer(resultC chan jobResultOrError) *workerResultsBuffer {
	return &workerResultsBuffer{
		resultC: resultC,
		results: make(map[string]*workerResult),
	}
}

func (b *workerResultsBuffer) gotAllResults() bool {
	for _, r := range b.results {
		if r == nil {
			return false
		}
	}
	return true
}

func (h *Hub) run() {
	for {
		select {
		case req := <-h.registerWorkerC:
			workerID := req.conn.workerID
			_, exists := h.workers[workerID]
			if exists {
				req.resultC <- false
				continue
			}
			h.workers[workerID] = req.conn
			ltsvlog.Logger.Info(ltsvlog.LV{"msg", "registered worker"},
				ltsvlog.LV{"worker_id", workerID},
				ltsvlog.LV{"worker_ids", h.WorkerIDs()})
			req.resultC <- true
		case conn := <-h.unregisterWorkerC:
			workerID := conn.workerID
			if h.workers[workerID] != conn {
				continue
			}
			delete(h.workers, workerID)
			close(conn.send)
			for _, b := range h.workerResultsBuffers {
				delete(b.results, workerID)
			}
			ltsvlog.Logger.Info(ltsvlog.LV{"msg", "unregistered worker"},
				ltsvlog.LV{"worker_id", workerID},
				ltsvlog.LV{"worker_ids", h.WorkerIDs()})
			for jobID, resultsBuf := range h.workerResultsBuffers {
				if resultsBuf.gotAllResults() {
					resultsBuf.resultC <- jobResultOrError{result: jobResult{results: resultsBuf.results}}
					delete(h.workerResultsBuffers, jobID)
				}
			}
		case req := <-h.broadcastToWorkersC:
			job := req.job
			job.ID = msg.JobID(atomic.AddUint64(&h.jobID, 1))
			message, err := msgpack.Marshal(msg.JobMsg, &job)
			if err != nil {
				ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
					ltsvlog.LV{"job", job},
					ltsvlog.LV{"err", err})
				return
			}
			resultsBuf := newWorkerResultsBuffer(req.resultC)
			for workerID, conn := range h.workers {
				select {
				case conn.send <- message:
					resultsBuf.results[workerID] = nil
				default:
					close(conn.send)
					delete(hub.workers, workerID)
				}
			}
			if len(resultsBuf.results) > 0 {
				h.workerResultsBuffers[job.ID] = resultsBuf
			} else {
				req.resultC <- jobResultOrError{err: errors.New("no worker")}
			}
		case res := <-h.workerResultToHubC:
			jobID := res.result.JobID
			resultsBuf := h.workerResultsBuffers[jobID]
			resultsBuf.results[res.workerID] = &res
			if resultsBuf.gotAllResults() {
				resultsBuf.resultC <- jobResultOrError{result: jobResult{results: resultsBuf.results}}
				delete(h.workerResultsBuffers, jobID)
			}
		}
	}
}

func (h *Hub) WorkerIDs() []string {
	workerIDs := make([]string, 0, len(h.workers))
	for workerID := range h.workers {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Sort(sort.StringSlice(workerIDs))
	return workerIDs
}
