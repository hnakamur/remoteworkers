package remoteworkers

import (
	"errors"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hnakamur/ltsvlog"
	"golang.org/x/net/context"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// WorkFunc is a function type for a worker to work.
type WorkFunc func(params interface{}) interface{}

// WorkerConfig is the configuration for workers.
type WorkerConfig struct {
	SendChannelLen          int
	DelayAfterSendingClose  time.Duration
	DelayBeforeReconnecting time.Duration
}

// DefaultWorkerConfig returns the default config for workers.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		SendChannelLen:          256,
		DelayAfterSendingClose:  time.Second,
		DelayBeforeReconnecting: time.Second,
	}
}

// Worker is the type for workers.
type Worker struct {
	serverURL               url.URL
	workerIDHeaderName      string
	workerID                string
	conn                    *websocket.Conn
	sendChannelLength       int
	sendC                   chan typeAndMessage
	doneC                   chan struct{}
	workFunc                WorkFunc
	delayAfterSendingClose  time.Duration
	delayBeforeReconnecting time.Duration
	logger                  ltsvlog.LogWriter
}

// NewWorker creates a worker.
func NewWorker(serverURL url.URL, workerIDHeaderName, workerID string, workFunc WorkFunc, logger ltsvlog.LogWriter, config WorkerConfig) *Worker {
	return &Worker{
		serverURL:               serverURL,
		workerIDHeaderName:      workerIDHeaderName,
		workerID:                workerID,
		sendChannelLength:       config.SendChannelLen,
		workFunc:                workFunc,
		delayAfterSendingClose:  config.DelayAfterSendingClose,
		delayBeforeReconnecting: config.DelayBeforeReconnecting,
		logger:                  logger,
	}
}

// writeMessage writes a message with the given messagepack message type, application message type and message.
func (w *Worker) writeMessage(c *websocket.Conn, mt int, tm typeAndMessage) error {
	writer, err := c.NextWriter(mt)
	if err != nil {
		w.logger.ErrorWithStack(ltsvlog.LV{"msg", "error in websocket.NextWriter"},
			ltsvlog.LV{"mt", mt},
			ltsvlog.LV{"err", err})
		return err
	}
	enc := msgpack.NewEncoder(writer)
	err = enc.Encode(tm.Type)
	if err != nil {
		w.logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
			ltsvlog.LV{"tm.Type", tm.Type},
			ltsvlog.LV{"err", err})
		return err
	}
	err = enc.Encode(tm.Message)
	if err != nil {
		w.logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
			ltsvlog.LV{"tm.Message", tm.Message},
			ltsvlog.LV{"err", err})
		return err
	}
	return writer.Close()
}

// Run runs a worker. The worker connects to the remote server over the websocket.
// Then it waits for a job to be sent from the server and does a work and sends
// the result to the server.
// If the connection is lost, the worker tries to reconnect to the server after
// the delay specified in config.DelayBeforeReconnecting passed to NewWorker.
func (w *Worker) Run(ctx context.Context) error {
	header := map[string][]string{
		w.workerIDHeaderName: []string{w.workerID},
	}
	for {
		w.doneC = make(chan struct{})
		w.sendC = make(chan typeAndMessage, w.sendChannelLength)
		errC := make(chan error)

		w.logger.Info(ltsvlog.LV{"msg", "connecting to server"}, ltsvlog.LV{"address", w.serverURL.String()})
		c, _, err := websocket.DefaultDialer.Dial(w.serverURL.String(), header)
		if err != nil {
			w.logger.Error(ltsvlog.LV{"msg", "dial error"},
				ltsvlog.LV{"address", w.serverURL.String()},
				ltsvlog.LV{"err", err},
			)
			goto retry_connect
		}
		defer c.Close()
		w.logger.Info(ltsvlog.LV{"msg", "connected to server"}, ltsvlog.LV{"address", w.serverURL.String()})
		w.conn = c

		go func() {
			err := w.readPump()
			if err != nil {
				errC <- err
			}
		}()

		for {
			select {
			case tm := <-w.sendC:
				err = w.writeMessage(c, websocket.BinaryMessage, tm)
				if err != nil {
					w.logger.ErrorWithStack(ltsvlog.LV{"msg", "write error"},
						ltsvlog.LV{"err", err})
					goto retry_connect
				}
			case <-w.doneC:
				if w.logger.DebugEnabled() {
					w.logger.Debug(ltsvlog.LV{"msg", "received from doneC"})
				}
				goto retry_connect

			case <-ctx.Done():
				w.logger.Info(ltsvlog.LV{"msg", "interrupt"})
				// To cleanly close a connection, a worker should sendC a close
				// frame and wait for the server to close the connection.
				err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				if err != nil {
					w.logger.ErrorWithStack(ltsvlog.LV{"msg", "write close error"},
						ltsvlog.LV{"err", err})
					return err
				}
				select {
				case <-w.doneC:
				case <-time.After(w.delayAfterSendingClose):
				}
				return ctx.Err()
			case <-errC:
				goto retry_connect
			}
		}

	retry_connect:
		select {
		case <-ctx.Done():
			w.logger.Info(ltsvlog.LV{"msg", "interrupt in retry_eonnect"})
			return ctx.Err()
		case <-time.After(w.delayBeforeReconnecting):
			if w.logger.DebugEnabled() {
				w.logger.Debug(ltsvlog.LV{"msg", "retrying connect to server"})
			}
		case <-errC:
			goto retry_connect
		}
	}
	return nil
}

func (w *Worker) readPump() error {
	defer close(w.doneC)
	for {
		wsMsgType, r, err := w.conn.NextReader()
		if err != nil {
			w.logger.Error(ltsvlog.LV{"msg", "read error"},
				ltsvlog.LV{"err", err})
			return err
		}
		if wsMsgType != websocket.BinaryMessage {
			w.logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected wsMsgType"},
				ltsvlog.LV{"wsMsgType", wsMsgType})
			return err
		}
		dec := msgpack.NewDecoder(r)
		var msgType messageType
		err = dec.Decode(&msgType)
		if err != nil {
			w.logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
				ltsvlog.LV{"err", err})
			return err
		}
		switch msgType {
		case registerWorkerResultMsg:
			var registerWorkerResult registerWorkerResultMessage
			err := dec.Decode(&registerWorkerResult)
			if err != nil {
				w.logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
					ltsvlog.LV{"err", err})
				return err
			}
			if registerWorkerResult.Error != "" {
				w.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to register worker"},
					ltsvlog.LV{"workerID", w.workerID},
					ltsvlog.LV{"err", registerWorkerResult.Error})
				return errors.New(registerWorkerResult.Error)
			}
			w.logger.Info(ltsvlog.LV{"msg", "registered myself as worker"},
				ltsvlog.LV{"workerID", w.workerID})
		case jobMsg:
			var job jobMessage
			err := dec.Decode(&job)
			if err != nil {
				w.logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
					ltsvlog.LV{"err", err})
				return err
			}

			if w.logger.DebugEnabled() {
				w.logger.Debug(ltsvlog.LV{"msg", "received Job"},
					ltsvlog.LV{"workerID", w.workerID},
					ltsvlog.LV{"job", job})
			}
			go func() {
				data := w.workFunc(job.Params)
				jobResult := workerResultMessage{
					JobID: job.ID,
					Data:  data,
				}
				w.sendC <- typeAndMessage{
					Type:    workerResultMsg,
					Message: &jobResult,
				}
			}()
		default:
			w.logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
				ltsvlog.LV{"messageType", msgType})
			return errors.New("unexpected MessageType")
		}
	}
}
