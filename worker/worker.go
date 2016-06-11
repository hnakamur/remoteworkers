package worker

import (
	"errors"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hnakamur/ltsvlog"
	"github.com/hnakamur/remoteworkers/msg"
	"golang.org/x/net/context"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type WorkFunc func(params interface{}) interface{}

type Worker struct {
	serverURL               url.URL
	workerIDHeaderName      string
	workerID                string
	conn                    *websocket.Conn
	sendChannelLength       int
	sendC                   chan []byte
	doneC                   chan struct{}
	workFunc                WorkFunc
	delayAfterSendingClose  time.Duration
	delayBeforeReconnecting time.Duration
	logger                  ltsvlog.LogWriter
}

func NewWorker(serverURL url.URL, workerIDHeaderName, workerID string, sendChannelLength int, workFunc WorkFunc, delayAfterSendingClose, delayBeforeReconnecting time.Duration, logger ltsvlog.LogWriter) *Worker {
	return &Worker{
		serverURL:               serverURL,
		workerIDHeaderName:      workerIDHeaderName,
		workerID:                workerID,
		sendChannelLength:       sendChannelLength,
		workFunc:                workFunc,
		delayAfterSendingClose:  delayAfterSendingClose,
		delayBeforeReconnecting: delayBeforeReconnecting,
		logger:                  logger,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	header := map[string][]string{
		w.workerIDHeaderName: []string{w.workerID},
	}
	for {
		w.doneC = make(chan struct{})
		w.sendC = make(chan []byte, w.sendChannelLength)
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
			case b := <-w.sendC:
				err = c.WriteMessage(websocket.BinaryMessage, b)
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
		var msgType msg.MessageType
		err = dec.Decode(&msgType)
		if err != nil {
			w.logger.ErrorWithStack(ltsvlog.LV{"msg", "decode error"},
				ltsvlog.LV{"err", err})
			return err
		}
		switch msgType {
		case msg.RegisterWorkerResultMsg:
			var registerWorkerResult msg.RegisterWorkerResult
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
		case msg.JobMsg:
			var job msg.Job
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
				jobResult := msg.WorkerResult{
					JobID: job.ID,
					Data:  data,
				}
				b, err := msgpack.Marshal(msg.WorkerResultMsg, &jobResult)
				if err != nil {
					w.logger.ErrorWithStack(ltsvlog.LV{"msg", "encode error"},
						ltsvlog.LV{"jobResult", jobResult},
						ltsvlog.LV{"err", err})
					return
				}
				w.sendC <- b
			}()
		default:
			w.logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
				ltsvlog.LV{"messageType", msgType})
			return errors.New("unexpected MessageType")
		}
	}
}
