package ws_surveyor

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
	logger ltsvlog.LogWriter

	// Registered workers.
	workers map[string]*Conn

	// Register worker requests from connections.
	registerWorkerC chan registerWorkerRequest

	// Unregister worker requests from connections.
	unregisterWorkerC chan *Conn

	// Inbound messages from the connections.
	broadcastToWorkersC chan jobRequestToHub

	// worker results buffers
	workerResultsBuffers map[msg.JobID]*workerResultsBuffer

	// channel for worker result
	workerResultToHubC chan workerResult

	// job ID
	jobID uint64
}

// NewHub creates a hub
func NewHub(logger ltsvlog.LogWriter) *Hub {
	return &Hub{
		logger:               logger,
		registerWorkerC:      make(chan registerWorkerRequest),
		unregisterWorkerC:    make(chan *Conn),
		workers:              make(map[string]*Conn),
		broadcastToWorkersC:  make(chan jobRequestToHub),
		workerResultsBuffers: make(map[msg.JobID]*workerResultsBuffer),
		workerResultToHubC:   make(chan workerResult),
	}
}

type registerWorkerRequest struct {
	conn    *Conn
	resultC chan bool
}

type jobRequestToHub struct {
	job     msg.Job
	resultC chan jobResultOrError
}

type jobResultOrError struct {
	jobID   msg.JobID
	results map[string]interface{}
	err     error
}

type workerResult struct {
	workerID string
	result   *msg.WorkerResult
}

type workerResultsBuffer struct {
	resultC chan jobResultOrError
	results map[string]*msg.WorkerResult
}

func newWorkerResultsBuffer(resultC chan jobResultOrError) *workerResultsBuffer {
	return &workerResultsBuffer{
		resultC: resultC,
		results: make(map[string]*msg.WorkerResult),
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

func (b *workerResultsBuffer) Results() map[string]interface{} {
	results := make(map[string]interface{})
	for workerID, r := range b.results {
		results[workerID] = r.Data
	}
	return results
}

// Run runs a hub
func (h *Hub) Run() {
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
			h.logger.Info(ltsvlog.LV{"msg", "registered worker"},
				ltsvlog.LV{"worker_id", workerID},
				ltsvlog.LV{"worker_ids", h.workerIDs()})
			req.resultC <- true
		case conn := <-h.unregisterWorkerC:
			workerID := conn.workerID
			if h.workers[workerID] != conn {
				continue
			}
			delete(h.workers, workerID)
			close(conn.sendC)
			for _, b := range h.workerResultsBuffers {
				delete(b.results, workerID)
			}
			h.logger.Info(ltsvlog.LV{"msg", "unregistered worker"},
				ltsvlog.LV{"worker_id", workerID},
				ltsvlog.LV{"worker_ids", h.workerIDs()})
			for jobID, resultsBuf := range h.workerResultsBuffers {
				if resultsBuf.gotAllResults() {
					resultsBuf.resultC <- jobResultOrError{jobID: jobID, results: resultsBuf.Results()}
					delete(h.workerResultsBuffers, jobID)
				}
			}
		case req := <-h.broadcastToWorkersC:
			job := req.job
			job.ID = msg.JobID(atomic.AddUint64(&h.jobID, 1))
			message, err := msgpack.Marshal(msg.JobMsg, &job)
			if err != nil {
				h.logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
					ltsvlog.LV{"job", job},
					ltsvlog.LV{"err", err})
				return
			}
			resultsBuf := newWorkerResultsBuffer(req.resultC)
			for workerID, conn := range h.workers {
				select {
				case conn.sendC <- message:
					resultsBuf.results[workerID] = nil
				default:
					close(conn.sendC)
					delete(h.workers, workerID)
				}
			}
			if len(resultsBuf.results) > 0 {
				h.workerResultsBuffers[job.ID] = resultsBuf
			} else {
				req.resultC <- jobResultOrError{jobID: job.ID, err: errors.New("no worker")}
			}
		case res := <-h.workerResultToHubC:
			jobID := res.result.JobID
			resultsBuf := h.workerResultsBuffers[jobID]
			resultsBuf.results[res.workerID] = res.result
			if resultsBuf.gotAllResults() {
				resultsBuf.resultC <- jobResultOrError{jobID: jobID, results: resultsBuf.Results()}
				delete(h.workerResultsBuffers, jobID)
			}
		}
	}
}

func (h *Hub) workerIDs() []string {
	workerIDs := make([]string, 0, len(h.workers))
	for workerID := range h.workers {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Sort(sort.StringSlice(workerIDs))
	return workerIDs
}

func (h *Hub) RequestWork(params interface{}) (map[string]interface{}, msg.JobID, error) {
	job := msg.Job{
		Params: params,
	}
	resultC := make(chan jobResultOrError)
	h.broadcastToWorkersC <- jobRequestToHub{
		job:     job,
		resultC: resultC,
	}

	res := <-resultC
	return res.results, res.jobID, res.err
}
